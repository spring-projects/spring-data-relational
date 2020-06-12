/*
 * Copyright 2020 the original author or authors.
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

import java.lang.reflect.Constructor;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcValue;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindingParser;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.Metadata;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.ParameterBinding;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A query to be executed based on a repository method, it's annotated SQL query
 * and the arguments provided to the method.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Mark Paluch
 * @author Christopher Klein
 * @since 2.0
 */
public class StringBasedJdbcQuery extends AbstractJdbcQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final JdbcQueryExecution<?> executor;
	private final JdbcConverter converter;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link StringBasedJdbcQuery} for the given
	 * {@link JdbcQueryMethod}, {@link RelationalMappingContext} and
	 * {@link RowMapper}.
	 *
	 * @param queryMethod      must not be {@literal null}.
	 * @param operations       must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying
	 *                         query).
	 */
	public StringBasedJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper, JdbcConverter converter,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(queryMethod, operations, defaultRowMapper);

		this.queryMethod = queryMethod;
		this.converter = converter;
		this.evaluationContextProvider = evaluationContextProvider;

		RowMapper<Object> rowMapper = determineRowMapper(defaultRowMapper);
		executor = getQueryExecution( //
				queryMethod, //
				determineResultSetExtractor(rowMapper != defaultRowMapper ? rowMapper : null), //
				rowMapper //
		);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.
	 * Object[])
	 */
	@Override
	public Object execute(Object[] objects) {
		
		Metadata queryMeta = new Metadata();

		String query = queryMethod.getDeclaredQuery();

		if (StringUtils.isEmpty(query)) {
			throw new IllegalStateException(String.format("No query specified on %s", queryMethod.getName()));
		}

		List<ParameterBinding> bindings = new ArrayList<>();

		query = ParameterBindingParser.INSTANCE.parseParameterBindingsOfQueryIntoBindingsAndReturnCleanedQuery(query,
				bindings, queryMeta);

		MapSqlParameterSource parameterMap = this.bindMethodParameters(objects);

		extendParametersFromSpELEvaluation(parameterMap, bindings, objects);
		return executor.execute(query, parameterMap);
	}

	/**
	 * Extend the {@link MapSqlParameterSource} by evaluating each detected SpEL
	 * parameter in the original query.
	 * 
	 * This is basically a simple variant of Spring Data JPA's SPeL implementation.
	 * 
	 * @param parameterMap
	 * @param bindings
	 * @param values
	 */
	void extendParametersFromSpELEvaluation(MapSqlParameterSource parameterMap, List<ParameterBinding> bindings, Object[] values) {
		
		if (bindings.size() == 0) {
			return;
		}

		ExpressionParser parser = new SpelExpressionParser();

		bindings.forEach(binding -> {
			if (!binding.isExpression()) {
				return;
			}

			Expression expression = parser.parseExpression(binding.getExpression());
			EvaluationContext context = evaluationContextProvider.getEvaluationContext(this.queryMethod.getParameters(),
					values);

			parameterMap.addValue(binding.getName(), expression.getValue(context, Object.class));
		});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}

	MapSqlParameterSource bindMethodParameters(Object[] objects) {
		MapSqlParameterSource parameters = new MapSqlParameterSource();

		queryMethod.getParameters().getBindableParameters()
				.forEach(p -> convertAndAddParameter(parameters, p, objects[p.getIndex()]));

		return parameters;
	}

	private void convertAndAddParameter(MapSqlParameterSource parameters, Parameter p, Object value) {

		String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));

		Class<?> parameterType = queryMethod.getParameters().getParameter(p.getIndex()).getType();
		Class<?> conversionTargetType = JdbcColumnTypes.INSTANCE.resolvePrimitiveType(parameterType);

		JdbcValue jdbcValue = converter.writeJdbcValue(value, conversionTargetType,
				JdbcUtil.sqlTypeFor(conversionTargetType));

		JDBCType jdbcType = jdbcValue.getJdbcType();
		if (jdbcType == null) {

			parameters.addValue(parameterName, jdbcValue.getValue());
		} else {
			parameters.addValue(parameterName, jdbcValue.getValue(), jdbcType.getVendorTypeNumber());
		}
	}

	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	ResultSetExtractor<Object> determineResultSetExtractor(@Nullable RowMapper<Object> rowMapper) {

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

	@SuppressWarnings("unchecked")
	RowMapper<Object> determineRowMapper(@Nullable RowMapper<?> defaultMapper) {

		Class<?> rowMapperClass = queryMethod.getRowMapperClass();

		if (isUnconfigured(rowMapperClass, RowMapper.class)) {
			return (RowMapper<Object>) defaultMapper;
		}

		return (RowMapper<Object>) BeanUtils.instantiateClass(rowMapperClass);
	}

	private static boolean isUnconfigured(@Nullable Class<?> configuredClass, Class<?> defaultClass) {
		return configuredClass == null || configuredClass == defaultClass;
	}
}
