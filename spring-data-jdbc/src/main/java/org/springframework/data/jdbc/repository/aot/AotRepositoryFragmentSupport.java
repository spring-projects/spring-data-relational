/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import java.lang.reflect.Method;
import java.sql.SQLType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.convert.DtoInstantiatingConverter;
import org.springframework.data.domain.Slice;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.query.JdbcParameters;
import org.springframework.data.jdbc.repository.query.JdbcValueBindUtil;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.jdbc.repository.query.StatementFactory;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.Lazy;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.ConcurrentLruCache;

/**
 * Support class for JDBC AOT repository fragments.
 * <p>
 * This class is indented to be used by generated AOT fragments and not to be used directly.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class AotRepositoryFragmentSupport {

	private static final ConversionService CONVERSION_SERVICE;

	static {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		conversionService.removeConvertible(Collection.class, Object.class);
		conversionService.removeConvertible(Object.class, Optional.class);

		CONVERSION_SERVICE = conversionService;
	}

	private final RowMapperFactory rowMapperFactory;

	private final JdbcAggregateOperations operations;

	private final StatementFactory statementFactory;

	private final ProjectionFactory projectionFactory;

	private final Lazy<ConcurrentLruCache<String, ValueExpression>> expressions;

	private final Lazy<ConcurrentLruCache<Method, JdbcParameters>> parameters;

	private final Lazy<ConcurrentLruCache<Method, ValueEvaluationContextProvider>> contextProviders;

	protected AotRepositoryFragmentSupport(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(operations, rowMapperFactory, context.getRepositoryMetadata(), context.getValueExpressionDelegate(),
				context.getProjectionFactory());
	}

	protected AotRepositoryFragmentSupport(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
			RepositoryMetadata repositoryMetadata, ValueExpressionDelegate valueExpressions,
			ProjectionFactory projectionFactory) {

		this.rowMapperFactory = rowMapperFactory;
		this.operations = operations;
		this.statementFactory = new StatementFactory(operations.getConverter(),
				operations.getDataAccessStrategy().getDialect());
		this.projectionFactory = projectionFactory;
		this.expressions = Lazy.of(() -> new ConcurrentLruCache<>(32, valueExpressions::parse));
		this.parameters = Lazy
				.of(() -> new ConcurrentLruCache<>(32, it -> new JdbcParameters(ParametersSource.of(repositoryMetadata, it))));
		this.contextProviders = Lazy.of(() -> new ConcurrentLruCache<>(32,
				it -> valueExpressions.createValueContextProvider(parameters.get().get(it))));
	}

	protected RowMapperFactory getRowMapperFactory() {
		return rowMapperFactory;
	}

	protected StatementFactory getStatementFactory() {
		return statementFactory;
	}

	protected Dialect getDialect() {
		return operations.getDataAccessStrategy().getDialect();
	}

	protected JdbcAggregateOperations getOperations() {
		return operations;
	}

	protected NamedParameterJdbcOperations getJdbcOperations() {
		return this.operations.getDataAccessStrategy().getJdbcOperations();
	}

	protected <T> @Nullable T queryForObject(String sql, SqlParameterSource paramSource,
			RowMapper<T> rowMapper) throws DataAccessException {

		List<T> results = getJdbcOperations().query(sql, paramSource, rowMapper);
		return DataAccessUtils.uniqueResult(results);
	}

	protected @Nullable Object escape(@Nullable Object value) {

		if (value == null) {
			return value;
		}

		return getDialect().getLikeEscaper().escape(value.toString());
	}

	protected BindValue getBindableValue(Method method, @Nullable Object value, String parameterReference) {
		return getBindableValue(parameters.get().get(method).getParameter(parameterReference), value);
	}

	protected BindValue getBindableValue(Method method, @Nullable Object value, int parameterIndex) {
		return getBindableValue(parameters.get().get(method).getParameter(parameterIndex), value);
	}

	private BindValue getBindableValue(JdbcParameters.JdbcParameter parameter, @Nullable Object value) {

		JdbcValue jdbcValue = JdbcValueBindUtil.getBindValue(operations.getConverter(), value, parameter);
		SQLType jdbcType = jdbcValue.getJdbcType();

		return (parameterName, parameterSource) -> {
			if (jdbcType == null) {
				parameterSource.addValue(parameterName, jdbcValue.getValue());
			} else {
				parameterSource.addValue(parameterName, jdbcValue.getValue(), jdbcType.getVendorTypeNumber());
			}
		};
	}

	protected BindValue evaluate(Method method, String expressionString, Object... args) {

		ValueExpression expression = this.expressions.get().get(expressionString);
		ValueEvaluationContextProvider contextProvider = this.contextProviders.get().get(method);

		ValueEvaluationContext context = contextProvider.getEvaluationContext(args, expression.getExpressionDependencies());
		Object evaluatedValue = expression.evaluate(context);
		Class<?> valueType = expression.getValueType(context);

		SQLType sqlType;

		if (valueType == null) {
			if (evaluatedValue != null) {
				sqlType = getSqlType(evaluatedValue.getClass());
			} else {
				sqlType = null;
			}
		} else {
			sqlType = getSqlType(valueType);
		}

		return (parameterName, parameterSource) -> {
			if (sqlType != null) {
				parameterSource.addValue(parameterName, evaluatedValue, sqlType.getVendorTypeNumber());
			} else {
				parameterSource.addValue(parameterName, evaluatedValue);
			}
		};
	}

	private static SQLType getSqlType(Class<?> valueType) {
		return JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(valueType));
	}

	protected <T> @Nullable T convertOne(@Nullable Object result, Class<T> projection) {

		if (result == null) {
			return null;
		}

		if (projection.isInstance(result)) {
			return projection.cast(result);
		}

		if (CONVERSION_SERVICE.canConvert(result.getClass(), projection)) {
			return CONVERSION_SERVICE.convert(result, projection);
		}

		if (!projection.isInterface()) {

			RelationalMappingContext mappingContext = operations.getConverter().getMappingContext();
			DtoInstantiatingConverter converter = new DtoInstantiatingConverter(projection, mappingContext,
					operations.getConverter().getEntityInstantiators());
			return (T) converter.convert(result);
		}

		return projectionFactory.createProjection(projection, result);
	}

	protected @Nullable Object convertMany(@Nullable Object result, Class<?> projection) {

		if (result == null) {
			return null;
		}

		if (projection.isInstance(result)) {
			return result;
		}

		if (result instanceof Stream<?> stream) {
			return stream.map(it -> convertOne(it, projection));
		}

		if (result instanceof Slice<?> slice) {
			return slice.map(it -> convertOne(it, projection));
		}

		if (result instanceof Collection<?> collection) {

			Collection<@Nullable Object> target = CollectionFactory.createCollection(collection.getClass(),
					collection.size());
			for (Object o : collection) {
				target.add(convertOne(o, projection));
			}

			return target;
		}

		throw new UnsupportedOperationException("Cannot create projection for %s".formatted(result));
	}

	/**
	 * Interface for binding values to a {@link MapSqlParameterSource}.
	 */
	protected interface BindValue {

		/**
		 * Bind the value to the given {@link MapSqlParameterSource} using the given parameter name. Can apply further value
		 * customization such as providing SQL types or type names.
		 *
		 * @param parameterName
		 * @param parameterSource
		 */
		void bind(String parameterName, MapSqlParameterSource parameterSource);

	}

}
