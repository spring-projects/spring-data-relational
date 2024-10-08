/*
 * Copyright 2020-2024 the original author or authors.
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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.ValueExpressionQueryRewriter;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

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
 * @author Christopher Klein
 * @author Mikhail Polivakha
 * @author Marcin Grzejszczak
 * @since 2.0
 */
public class StringBasedJdbcQuery extends AbstractJdbcQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters; Use @Param for query method parameters, or use the javac flag -parameters";
	private final JdbcConverter converter;
	private final RowMapperFactory rowMapperFactory;
	private final ValueExpressionQueryRewriter.ParsedQuery parsedQuery;
	private final String query;

	private final CachedRowMapperFactory cachedRowMapperFactory;
	private final CachedResultSetExtractorFactory cachedResultSetExtractorFactory;
	private final ValueExpressionDelegate delegate;

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapper}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
	 * @deprecated since 3.4, use the constructors accepting {@link ValueExpressionDelegate} instead.
	 */
	@Deprecated(since = "3.4")
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper, JdbcConverter converter,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(queryMethod.getRequiredQuery(), queryMethod, operations, result -> (RowMapper<Object>) defaultRowMapper,
				converter, evaluationContextProvider);
	}

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapperFactory}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @since 2.3
	 * @deprecated use alternative constructor
	 */
	@Deprecated(since = "3.4")
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			RowMapperFactory rowMapperFactory, JdbcConverter converter,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(queryMethod.getRequiredQuery(), queryMethod, operations, rowMapperFactory, converter,
				evaluationContextProvider);
	}

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapperFactory}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 * @since 3.4
	 */
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			RowMapperFactory rowMapperFactory, JdbcConverter converter,
			ValueExpressionDelegate delegate) {
		this(queryMethod.getRequiredQuery(), queryMethod, operations, rowMapperFactory, converter, delegate);
	}

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapperFactory}.
	 *
	 * @param query must not be {@literal null} or empty.
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 * @since 3.4
	 */
	public StringBasedJdbcQuery(String query, JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			RowMapperFactory rowMapperFactory, JdbcConverter converter,
			ValueExpressionDelegate delegate) {
		super(queryMethod, operations);
		Assert.hasText(query, "Query must not be null or empty");
		Assert.notNull(rowMapperFactory, "RowMapperFactory must not be null");

		this.converter = converter;
		this.rowMapperFactory = rowMapperFactory;

		if (queryMethod.isSliceQuery()) {
			throw new UnsupportedOperationException(
					"Slice queries are not supported using string-based queries; Offending method: " + queryMethod);
		}

		if (queryMethod.isPageQuery()) {
			throw new UnsupportedOperationException(
					"Page queries are not supported using string-based queries; Offending method: " + queryMethod);
		}

		if (queryMethod.getParameters().hasLimitParameter()) {
			throw new UnsupportedOperationException(
					"Queries with Limit are not supported using string-based queries; Offending method: " + queryMethod);
		}

		this.cachedRowMapperFactory = new CachedRowMapperFactory(
				() -> rowMapperFactory.create(queryMethod.getResultProcessor().getReturnedType().getReturnedType()));
		this.cachedResultSetExtractorFactory = new CachedResultSetExtractorFactory(
				this.cachedRowMapperFactory::getRowMapper);

		ValueExpressionQueryRewriter rewriter = ValueExpressionQueryRewriter.of(delegate,
				(counter, expression) -> String.format("__$synthetic$__%d", counter + 1), String::concat);

		this.query = query;
		this.parsedQuery = rewriter.parse(this.query);
		this.delegate = delegate;
	}

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext}
	 * and {@link RowMapperFactory}.
	 *
	 * @param query must not be {@literal null} or empty.
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @since 3.4
	 * @deprecated since 3.4, use the constructors accepting {@link ValueExpressionDelegate} instead.
	 */
	@Deprecated(since = "3.4")
	public StringBasedJdbcQuery(String query, JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			RowMapperFactory rowMapperFactory, JdbcConverter converter,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(query, queryMethod, operations, rowMapperFactory, converter, new CachingValueExpressionDelegate(
				new QueryMethodValueEvaluationContextAccessor(new StandardEnvironment(), rootObject -> evaluationContextProvider
						.getEvaluationContext(queryMethod.getParameters(), new Object[] { rootObject })),
				ValueExpressionParser.create()));
	}

	@Override
	public Object execute(Object[] objects) {

		RelationalParameterAccessor accessor = new RelationalParametersParameterAccessor(getQueryMethod(), objects);
		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);

		JdbcQueryExecution<?> queryExecution = createJdbcQueryExecution(accessor, processor);
		MapSqlParameterSource parameterMap = this.bindParameters(accessor);

		return queryExecution.execute(evaluateExpressions(objects, accessor.getBindableParameters(), parameterMap),
				parameterMap);
	}

	private String evaluateExpressions(Object[] objects, Parameters<?, ?> bindableParameters,
			MapSqlParameterSource parameterMap) {

		if (parsedQuery.hasParameterBindings()) {

			ValueEvaluationContext evaluationContext = delegate.createValueContextProvider(bindableParameters)
					.getEvaluationContext(objects);

			parsedQuery.getParameterMap().forEach((paramName, valueExpression) -> {
				parameterMap.addValue(paramName, valueExpression.evaluate(evaluationContext));
			});

			return parsedQuery.getQueryString();
		}

		return this.query;
	}

	private JdbcQueryExecution<?> createJdbcQueryExecution(RelationalParameterAccessor accessor,
			ResultProcessor processor) {

		if (getQueryMethod().isModifyingQuery()) {
			return createModifyingQueryExecutor();
		}

		Supplier<RowMapper<?>> rowMapper = () -> determineRowMapper(processor, accessor.findDynamicProjection() != null);
		ResultSetExtractor<Object> resultSetExtractor = determineResultSetExtractor(rowMapper);

		return createReadingQueryExecution(resultSetExtractor, rowMapper);
	}

	private MapSqlParameterSource bindParameters(RelationalParameterAccessor accessor) {

		Parameters<?, ?> bindableParameters = accessor.getBindableParameters();
		MapSqlParameterSource parameters = new MapSqlParameterSource(
				new LinkedHashMap<>(bindableParameters.getNumberOfParameters(), 1.0f));

		for (Parameter bindableParameter : bindableParameters) {

			Object value = accessor.getBindableValue(bindableParameter.getIndex());
			String parameterName = bindableParameter.getName()
					.orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			JdbcParameters.JdbcParameter parameter = getQueryMethod().getParameters()
					.getParameter(bindableParameter.getIndex());

			JdbcValue jdbcValue = writeValue(value, parameter.getTypeInformation(), parameter);
			SQLType jdbcType = jdbcValue.getJdbcType();

			if (jdbcType == null) {
				parameters.addValue(parameterName, jdbcValue.getValue());
			} else {
				parameters.addValue(parameterName, jdbcValue.getValue(), jdbcType.getVendorTypeNumber());
			}
		}

		return parameters;
	}

	private JdbcValue writeValue(@Nullable Object value, TypeInformation<?> typeInformation,
			JdbcParameters.JdbcParameter parameter) {

		if (value == null) {
			return JdbcValue.of(value, parameter.getSqlType());
		}

		if (typeInformation.isCollectionLike() && value instanceof Collection<?> collection) {

			TypeInformation<?> actualType = typeInformation.getActualType();

			// allow tuple-binding for collection of byte arrays to be used as BINARY,
			// we do not want to convert to column arrays.
			if (actualType != null && actualType.getType().isArray() && !actualType.getType().equals(byte[].class)) {

				TypeInformation<?> nestedElementType = actualType.getRequiredActualType();
				return writeCollection(collection, parameter.getActualSqlType(),
						array -> writeArrayValue(parameter, array, nestedElementType));
			}

			// parameter expansion
			return writeCollection(collection, parameter.getActualSqlType(),
					it -> converter.writeJdbcValue(it, typeInformation.getRequiredActualType(), parameter.getActualSqlType()));
		}

		SQLType sqlType = parameter.getSqlType();
		return converter.writeJdbcValue(value, typeInformation, sqlType);
	}

	private JdbcValue writeCollection(Collection<?> value, SQLType defaultType, Function<Object, Object> mapper) {

		if (value.isEmpty()) {
			return JdbcValue.of(value, defaultType);
		}

		JdbcValue jdbcValue;
		List<Object> mapped = new ArrayList<>(value.size());
		SQLType jdbcType = null;

		for (Object o : value) {

			Object mappedValue = mapper.apply(o);

			if (mappedValue instanceof JdbcValue jv) {
				if (jdbcType == null) {
					jdbcType = jv.getJdbcType();
				}
				mappedValue = jv.getValue();
			}

			mapped.add(mappedValue);
		}

		jdbcValue = JdbcValue.of(mapped, jdbcType == null ? defaultType : jdbcType);

		return jdbcValue;
	}

	private JdbcValue writeArrayValue(JdbcParameters.JdbcParameter parameter, Object array,
			TypeInformation<?> nestedElementType) {

		int length = Array.getLength(array);
		Object[] mappedArray = new Object[length];
		SQLType sqlType = null;

		for (int i = 0; i < length; i++) {

			Object element = Array.get(array, i);
			JdbcValue converted = converter.writeJdbcValue(element, nestedElementType, parameter.getActualSqlType());

			if (sqlType == null && converted.getJdbcType() != null) {
				sqlType = converted.getJdbcType();
			}
			mappedArray[i] = converted.getValue();
		}

		if (sqlType == null) {
			sqlType = JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(nestedElementType.getType()));
		}

		return JdbcValue.of(mappedArray, sqlType);
	}

	RowMapper<Object> determineRowMapper(ResultProcessor resultProcessor, boolean hasDynamicProjection) {

		if (cachedRowMapperFactory.isConfiguredRowMapper()) {
			return cachedRowMapperFactory.getRowMapper();
		}

		if (hasDynamicProjection) {

			RowMapper<Object> rowMapperToUse = rowMapperFactory.create(resultProcessor.getReturnedType().getDomainType());

			ResultProcessingConverter converter = new ResultProcessingConverter(resultProcessor,
					this.converter.getMappingContext(), this.converter.getEntityInstantiators());
			return new ConvertingRowMapper<>(rowMapperToUse, converter);
		}

		return cachedRowMapperFactory.getRowMapper();
	}

	@Nullable
	ResultSetExtractor<Object> determineResultSetExtractor(Supplier<RowMapper<?>> rowMapper) {

		if (cachedResultSetExtractorFactory.isConfiguredResultSetExtractor()) {

			if (cachedResultSetExtractorFactory.requiresRowMapper() && !cachedRowMapperFactory.isConfiguredRowMapper()) {
				return cachedResultSetExtractorFactory.getResultSetExtractor(rowMapper);
			}

			// configured ResultSetExtractor defaults to configured RowMapper in case both are configured
			return cachedResultSetExtractorFactory.getResultSetExtractor();
		}

		return null;
	}

	private static boolean isUnconfigured(@Nullable Class<?> configuredClass, Class<?> defaultClass) {
		return configuredClass == null || configuredClass == defaultClass;
	}

	@Deprecated(since = "3.4")
	public void setBeanFactory(BeanFactory beanFactory) {}

	class CachedRowMapperFactory {

		private final Lazy<RowMapper<Object>> cachedRowMapper;
		private final boolean configuredRowMapper;
		private final @Nullable Constructor<?> constructor;

		@SuppressWarnings("unchecked")
		public CachedRowMapperFactory(Supplier<RowMapper<Object>> defaultMapper) {

			String rowMapperRef = getQueryMethod().getRowMapperRef();
			Class<?> rowMapperClass = getQueryMethod().getRowMapperClass();

			if (!ObjectUtils.isEmpty(rowMapperRef) && !isUnconfigured(rowMapperClass, RowMapper.class)) {
				throw new IllegalArgumentException(
						"Invalid RowMapper configuration. Configure either one but not both via @Query(rowMapperRef = …, rowMapperClass = …) for query method "
								+ getQueryMethod());
			}

			this.configuredRowMapper = !ObjectUtils.isEmpty(rowMapperRef) || !isUnconfigured(rowMapperClass, RowMapper.class);
			this.constructor = rowMapperClass != null ? findPrimaryConstructor(rowMapperClass) : null;
			this.cachedRowMapper = Lazy.of(() -> {

				if (!ObjectUtils.isEmpty(rowMapperRef)) {
					return rowMapperFactory.getRowMapper(rowMapperRef);
				}

				if (isUnconfigured(rowMapperClass, RowMapper.class)) {
					return defaultMapper.get();
				}

				return (RowMapper<Object>) BeanUtils.instantiateClass(constructor);
			});
		}

		public boolean isConfiguredRowMapper() {
			return configuredRowMapper;
		}

		public RowMapper<Object> getRowMapper() {
			return cachedRowMapper.get();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	class CachedResultSetExtractorFactory {

		private final Lazy<ResultSetExtractor<Object>> cachedResultSetExtractor;
		private final boolean configuredResultSetExtractor;
		private final @Nullable Constructor<? extends ResultSetExtractor> rowMapperConstructor;
		private final @Nullable Constructor<? extends ResultSetExtractor> constructor;
		private final Function<Supplier<RowMapper<?>>, ResultSetExtractor<Object>> resultSetExtractorFactory;

		public CachedResultSetExtractorFactory(Supplier<RowMapper<?>> resultSetExtractor) {

			String resultSetExtractorRef = getQueryMethod().getResultSetExtractorRef();
			Class<? extends ResultSetExtractor> resultSetExtractorClass = getQueryMethod().getResultSetExtractorClass();

			if (!ObjectUtils.isEmpty(resultSetExtractorRef)
					&& !isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class)) {
				throw new IllegalArgumentException(
						"Invalid ResultSetExtractor configuration. Configure either one but not both via @Query(resultSetExtractorRef = …, resultSetExtractorClass = …) for query method "
								+ getQueryMethod());
			}

			this.configuredResultSetExtractor = !ObjectUtils.isEmpty(resultSetExtractorRef)
					|| !isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class);

			this.rowMapperConstructor = resultSetExtractorClass != null
					? ClassUtils.getConstructorIfAvailable(resultSetExtractorClass, RowMapper.class)
					: null;
			this.constructor = resultSetExtractorClass != null ? findPrimaryConstructor(resultSetExtractorClass) : null;
			this.resultSetExtractorFactory = rowMapper -> {

				if (!ObjectUtils.isEmpty(resultSetExtractorRef)) {
					return rowMapperFactory.getResultSetExtractor(resultSetExtractorRef);
				}

				if (isUnconfigured(resultSetExtractorClass, ResultSetExtractor.class)) {
					throw new UnsupportedOperationException("This should not happen");
				}

				if (rowMapperConstructor != null) {
					return BeanUtils.instantiateClass(rowMapperConstructor, rowMapper.get());
				}

				return BeanUtils.instantiateClass(constructor);
			};

			this.cachedResultSetExtractor = Lazy.of(() -> resultSetExtractorFactory.apply(resultSetExtractor));
		}

		public boolean isConfiguredResultSetExtractor() {
			return configuredResultSetExtractor;
		}

		public ResultSetExtractor<Object> getResultSetExtractor() {
			return cachedResultSetExtractor.get();
		}

		public ResultSetExtractor<Object> getResultSetExtractor(Supplier<RowMapper<?>> rowMapperSupplier) {
			return resultSetExtractorFactory.apply(rowMapperSupplier);
		}

		public boolean requiresRowMapper() {
			return rowMapperConstructor != null;
		}
	}

	@Nullable
	static <T> Constructor<T> findPrimaryConstructor(Class<T> clazz) {
		try {
			return clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException ex) {
			return BeanUtils.findPrimaryConstructor(clazz);

		} catch (LinkageError err) {
			throw new BeanInstantiationException(clazz, "Unresolvable class definition", err);
		}
	}
}
