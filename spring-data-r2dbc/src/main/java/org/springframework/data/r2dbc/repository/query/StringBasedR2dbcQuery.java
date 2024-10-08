/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.expression.ReactiveValueEvaluationContextProvider;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.BindTargetBinder;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.util.Assert;

/**
 * String-based {@link StringBasedR2dbcQuery} implementation.
 * <p>
 * A {@link StringBasedR2dbcQuery} expects a query method to be annotated with {@link Query} with a SQL query. Supports
 * named parameters (if enabled on {@link DatabaseClient}) and SpEL expressions enclosed with {@code :#{…}}.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 */
public class StringBasedR2dbcQuery extends AbstractR2dbcQuery {

	private final ExpressionQuery expressionQuery;
	private final ExpressionEvaluatingParameterBinder binder;
	private final ExpressionDependencies expressionDependencies;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final ReactiveValueEvaluationContextProvider valueContextProvider;

	/**
	 * Creates a new {@link StringBasedR2dbcQuery} for the given {@link StringBasedR2dbcQuery}, {@link DatabaseClient},
	 * {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @deprecated use the constructor version with {@link ValueExpressionDelegate}
	 */
	@Deprecated(since = "3.4")
	public StringBasedR2dbcQuery(R2dbcQueryMethod queryMethod, R2dbcEntityOperations entityOperations,
			R2dbcConverter converter, ReactiveDataAccessStrategy dataAccessStrategy, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, entityOperations, converter, dataAccessStrategy,
				expressionParser, evaluationContextProvider);
	}

	/**
	 * Create a new {@link StringBasedR2dbcQuery} for the given {@code query}, {@link R2dbcQueryMethod},
	 * {@link DatabaseClient}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param query must not be {@literal null}.
	 * @param method must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @deprecated use the constructor version with {@link ValueExpressionDelegate}
	 */
	@Deprecated(since = "3.4")
	public StringBasedR2dbcQuery(String query, R2dbcQueryMethod method, R2dbcEntityOperations entityOperations,
			R2dbcConverter converter, ReactiveDataAccessStrategy dataAccessStrategy, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(query, method, entityOperations, converter, dataAccessStrategy, new ValueExpressionDelegate(new QueryMethodValueEvaluationContextAccessor(new StandardEnvironment(), evaluationContextProvider.getEvaluationContextProvider()), ValueExpressionParser.create(() -> expressionParser)));
	}

	/**
	 * Create a new {@link StringBasedR2dbcQuery} for the given {@code query}, {@link R2dbcQueryMethod},
	 * {@link DatabaseClient}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param valueExpressionDelegate must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(R2dbcQueryMethod method, R2dbcEntityOperations entityOperations,
			R2dbcConverter converter, ReactiveDataAccessStrategy dataAccessStrategy, ValueExpressionDelegate valueExpressionDelegate) {
		this(method.getRequiredAnnotatedQuery(), method, entityOperations, converter, dataAccessStrategy, valueExpressionDelegate);
	}

	/**
	 * Create a new {@link StringBasedR2dbcQuery} for the given {@code query}, {@link R2dbcQueryMethod},
	 * {@link DatabaseClient}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param valueExpressionDelegate must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(String query, R2dbcQueryMethod method, R2dbcEntityOperations entityOperations,
			R2dbcConverter converter, ReactiveDataAccessStrategy dataAccessStrategy, ValueExpressionDelegate valueExpressionDelegate) {

		super(method, entityOperations, converter);

		Assert.hasText(query, "Query must not be empty");

		this.dataAccessStrategy = dataAccessStrategy;
		this.expressionQuery = ExpressionQuery.create(valueExpressionDelegate, query);
		this.binder = new ExpressionEvaluatingParameterBinder(expressionQuery, dataAccessStrategy);

		ValueEvaluationContextProvider valueContextProvider = valueExpressionDelegate
				.createValueContextProvider(method.getParameters());
		Assert.isInstanceOf(ReactiveValueEvaluationContextProvider.class, valueContextProvider,
				"ValueEvaluationContextProvider must be reactive");

		this.valueContextProvider = (ReactiveValueEvaluationContextProvider) valueContextProvider;
		this.expressionDependencies = createExpressionDependencies();


		if (method.isSliceQuery()) {
			throw new UnsupportedOperationException(
					"Slice queries are not supported using string-based queries; Offending method: " + method);
		}

		if (method.isPageQuery()) {
			throw new UnsupportedOperationException(
					"Page queries are not supported using string-based queries; Offending method: " + method);
		}

		if (method.getParameters().hasLimitParameter()) {
			throw new UnsupportedOperationException(
					"Queries with Limit are not supported using string-based queries; Offending method: " + method);
		}
	}

	private ExpressionDependencies createExpressionDependencies() {

		if (expressionQuery.getBindings().isEmpty()) {
			return ExpressionDependencies.none();
		}

		List<ExpressionDependencies> dependencies = new ArrayList<>();

		expressionQuery.getBindings()
				.forEach((s, valueExpression) -> dependencies.add(valueExpression.getExpressionDependencies()));

		return ExpressionDependencies.merged(dependencies);
	}

	@Override
	protected boolean isModifyingQuery() {
		return getQueryMethod().isModifyingQuery();
	}

	@Override
	protected boolean isCountQuery() {
		return false;
	}

	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	@Override
	protected Mono<PreparedOperation<?>> createQuery(RelationalParameterAccessor accessor) {
		return getExpressionEvaluator(accessor).map(evaluator -> new ExpandedQuery(accessor, evaluator));
	}

	@Override
	Class<?> resolveResultType(ResultProcessor resultProcessor) {

		Class<?> returnedType = resultProcessor.getReturnedType().getReturnedType();
		return !returnedType.isInterface() ? returnedType : super.resolveResultType(resultProcessor);
	}

	private Mono<ValueEvaluationContext> getExpressionEvaluator(RelationalParameterAccessor accessor) {
		return valueContextProvider.getEvaluationContextLater(accessor.getValues(), expressionDependencies);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [" + expressionQuery.getQuery() + ']';
	}

	private class ExpandedQuery implements PreparedOperation<String> {

		private final BindTargetRecorder recordedBindings;

		private final PreparedOperation<?> expanded;

		private final Map<String, Parameter> remainderByName;

		private final Map<Integer, Parameter> remainderByIndex;

		public ExpandedQuery(RelationalParameterAccessor accessor, ValueEvaluationContext evaluationContext) {

			this.recordedBindings = new BindTargetRecorder();
			binder.bind(recordedBindings, accessor, evaluationContext);

			remainderByName = new LinkedHashMap<>(recordedBindings.byName);
			remainderByIndex = new LinkedHashMap<>(recordedBindings.byIndex);
			expanded = dataAccessStrategy.processNamedParameters(expressionQuery.getQuery(), (index, name) -> {

				if (recordedBindings.byName.containsKey(name)) {
					remainderByName.remove(name);
					return recordedBindings.byName.get(name);
				}

				if (recordedBindings.byIndex.containsKey(index)) {
					remainderByIndex.remove(index);
					return recordedBindings.byIndex.get(index);
				}

				return null;
			});
		}

		@Override
		public String getSource() {
			return expressionQuery.getQuery();
		}

		@Override
		public void bindTo(BindTarget target) {

			BindTargetBinder binder = new BindTargetBinder(target);
			expanded.bindTo(target);

			remainderByName.forEach(binder::bind);
			remainderByIndex.forEach(binder::bind);
		}

		@Override
		public String toQuery() {
			return expanded.toQuery();
		}

		@Override
		public String toString() {
			return String.format("Original: [%s], Expanded: [%s]", expressionQuery.getQuery(), expanded.toQuery());
		}
	}

	private static class BindTargetRecorder implements BindTarget {

		final Map<Integer, Parameter> byIndex = new LinkedHashMap<>();

		final Map<String, Parameter> byName = new LinkedHashMap<>();

		@Override
		public void bind(String identifier, Object value) {
			byName.put(identifier, toParameter(value));
		}

		private Parameter toParameter(Object value) {
			return value instanceof Parameter ? (Parameter) value : Parameter.from(value);
		}

		@Override
		public void bind(int index, Object value) {
			byIndex.put(index, toParameter(value));
		}

		@Override
		public void bindNull(String identifier, Class<?> type) {
			byName.put(identifier, Parameter.empty(type));
		}

		@Override
		public void bindNull(int index, Class<?> type) {
			byIndex.put(index, Parameter.empty(type));
		}
	}
}
