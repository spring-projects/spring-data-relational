/*
 * Copyright 2020-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.query;

import static org.springframework.data.jdbc.repository.query.JdbcQueryExecution.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * A query to be executed based on a repository method, it's annotated SQL query and the arguments provided to the
 * method.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Mark Paluch
 * @author Hebert Coelho
 * @author Chirag Tailor
 * @author Zhou Xingyii
 * @since 2.0
 */
public class StringBasedJdbcQuery extends AbstractJdbcQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final JdbcConverter converter;
	private final RowMapperFactory rowMapperFactory;
	private BeanFactory beanFactory;

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapper}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
	 */
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper, JdbcConverter converter) {
		this(queryMethod, operations, result -> (RowMapper<Object>) defaultRowMapper, converter);
	}

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapperFactory}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @since 2.3
	 */
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			RowMapperFactory rowMapperFactory, JdbcConverter converter) {

		super(queryMethod, operations);

		Assert.notNull(rowMapperFactory, "RowMapperFactory must not be null");

		this.queryMethod = queryMethod;
		this.converter = converter;
		this.rowMapperFactory = rowMapperFactory;

		if (queryMethod.isSliceQuery()) {
			throw new UnsupportedOperationException(
					"Slice queries are not supported using string-based queries. Offending method: " + queryMethod);
		}

		if (queryMethod.isPageQuery()) {
			throw new UnsupportedOperationException(
					"Page queries are not supported using string-based queries. Offending method: " + queryMethod);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] objects) {

		RelationalParameterAccessor accessor = new RelationalParametersParameterAccessor(getQueryMethod(), objects);
		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);
		ResultProcessingConverter converter = new ResultProcessingConverter(processor, this.converter.getMappingContext(),
				this.converter.getEntityInstantiators());

		RowMapper<Object> rowMapper = determineRowMapper(rowMapperFactory.create(resolveTypeToRead(processor)), converter,
				accessor.findDynamicProjection() != null);

		JdbcQueryExecution<?> queryExecution = getQueryExecution(//
				queryMethod, //
				determineResultSetExtractor(rowMapper), //
				rowMapper);

		return queryExecution.execute(determineQuery(), this.bindParameters(accessor));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}

	private MapSqlParameterSource bindParameters(RelationalParameterAccessor accessor) {

		MapSqlParameterSource parameters = new MapSqlParameterSource();

		Parameters<?, ?> bindableParameters = accessor.getBindableParameters();

		for (Parameter bindableParameter : bindableParameters) {
			convertAndAddParameter(parameters, bindableParameter, accessor.getBindableValue(bindableParameter.getIndex()));
		}

		return parameters;
	}

	private void convertAndAddParameter(MapSqlParameterSource parameters, Parameter p, Object value) {

		String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));

		final Field methodParameterField = ReflectionUtils.findField(Parameter.class, "parameter", MethodParameter.class);
		Assert.state(methodParameterField != null, "MethodParameter must not be null");
		ReflectionUtils.makeAccessible(methodParameterField);
		final MethodParameter methodParameter = (MethodParameter) ReflectionUtils.getField(methodParameterField, p);
		if (methodParameter != null && methodParameter.hasParameterAnnotation(Multiparameter.class)) {
			if (value instanceof Map<?, ?>) {
				final Map<?, ?> m = (Map<?, ?>) value;
				m.forEach((propertyName, v) -> parameters.addValue(parameterName + '.' + propertyName, v));
			} else {
				final BeanPropertySqlParameterSource parameterSource = new BeanPropertySqlParameterSource(value);
				for (String propertyName : parameterSource.getParameterNames()) {
					final String newParameterName = parameterName + '.' + propertyName;
					final Object parameterValue = parameterSource.getValue(propertyName);
					final int sqlType = parameterSource.getSqlType(propertyName);
					final String typeName = parameterSource.getTypeName(propertyName);
					if (typeName == null) {
						parameters.addValue(newParameterName, parameterValue, sqlType);
					} else {
						parameters.addValue(newParameterName, parameterValue, sqlType, typeName);
					}
				}
			}
		} else {
			RelationalParameters.RelationalParameter parameter = queryMethod.getParameters().getParameter(p.getIndex());
			ResolvableType resolvableType = parameter.getResolvableType();
			Class<?> type = resolvableType.resolve();
			Assert.notNull(type, "@Query parameter type could not be resolved!");

			JdbcValue jdbcValue;
			if (value instanceof Iterable) {

				List<Object> mapped = new ArrayList<>();
				SQLType jdbcType = null;

				Class<?> elementType = resolvableType.getGeneric(0).resolve();

				Assert.notNull(elementType, "@Query Iterable parameter generic type could not be resolved!");

				for (Object o : (Iterable<?>) value) {
					JdbcValue elementJdbcValue = converter.writeJdbcValue(o, elementType,
							JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(elementType)));
					if (jdbcType == null) {
						jdbcType = elementJdbcValue.getJdbcType();
					}

					mapped.add(elementJdbcValue.getValue());
				}

				jdbcValue = JdbcValue.of(mapped, jdbcType);
			} else {
				jdbcValue = converter.writeJdbcValue(value, type,
						JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(type)));
			}

			SQLType jdbcType = jdbcValue.getJdbcType();
			if (jdbcType == null) {

				parameters.addValue(parameterName, jdbcValue.getValue());
			} else {
				parameters.addValue(parameterName, jdbcValue.getValue(), jdbcType.getVendorTypeNumber());
			}
		}
	}

	private String determineQuery() {

		String query = queryMethod.getDeclaredQuery();

		if (ObjectUtils.isEmpty(query)) {
			throw new IllegalStateException(String.format("No query specified on %s", queryMethod.getName()));
		}

		return query;
	}

	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	ResultSetExtractor<Object> determineResultSetExtractor(@Nullable RowMapper<Object> rowMapper) {

		String resultSetExtractorRef = queryMethod.getResultSetExtractorRef();

		if (!StringUtils.isEmpty(resultSetExtractorRef)) {

			Assert.notNull(beanFactory, "When a ResultSetExtractorRef is specified the BeanFactory must not be null");

			return (ResultSetExtractor<Object>) beanFactory.getBean(resultSetExtractorRef);
		}

		Class<? extends ResultSetExtractor> resultSetExtractorClass = queryMethod.getResultSetExtractorClass();

		if (isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class)) {
			return null;
		}

		Constructor<? extends ResultSetExtractor> constructor = ClassUtils
				.getConstructorIfAvailable(resultSetExtractorClass, RowMapper.class);

		if (constructor != null) {
			return BeanUtils.instantiateClass(constructor, rowMapper);
		}

		return BeanUtils.instantiateClass(resultSetExtractorClass);
	}

	@Nullable
	RowMapper<Object> determineRowMapper(@Nullable RowMapper<?> defaultMapper,
			Converter<Object, Object> resultProcessingConverter, boolean hasDynamicProjection) {

		RowMapper<Object> rowMapperToUse = determineRowMapper(defaultMapper);

		if ((hasDynamicProjection || rowMapperToUse == defaultMapper) && rowMapperToUse != null) {
			return new ConvertingRowMapper<>(rowMapperToUse, resultProcessingConverter);
		}

		return rowMapperToUse;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	RowMapper<Object> determineRowMapper(@Nullable RowMapper<?> defaultMapper) {

		String rowMapperRef = queryMethod.getRowMapperRef();

		if (!StringUtils.isEmpty(rowMapperRef)) {

			Assert.notNull(beanFactory, "When a RowMapperRef is specified the BeanFactory must not be null");

			return (RowMapper<Object>) beanFactory.getBean(rowMapperRef);
		}

		Class<?> rowMapperClass = queryMethod.getRowMapperClass();

		if (isUnconfigured(rowMapperClass, RowMapper.class)) {
			return (RowMapper<Object>) defaultMapper;
		}

		return (RowMapper<Object>) BeanUtils.instantiateClass(rowMapperClass);
	}

	private static boolean isUnconfigured(@Nullable Class<?> configuredClass, Class<?> defaultClass) {
		return configuredClass == null || configuredClass == defaultClass;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
}
