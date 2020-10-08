/*
 * Copyright 2018-2020 the original author or authors.
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
import java.util.List;

import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.Assert;

/**
 * String-based {@link StringBasedR2dbcQuery} implementation.
 * <p>
 * A {@link StringBasedR2dbcQuery} expects a query method to be annotated with {@link Query} with a SQL query. Supports
 * named parameters (if enabled on {@link DatabaseClient}) and SpEL expressions enclosed with {@code :#{…}}.
 *
 * @author Mark Paluch
 */
public class StringBasedR2dbcQuery extends AbstractR2dbcQuery {

	private final ExpressionQuery expressionQuery;
	private final ExpressionEvaluatingParameterBinder binder;
	private final ExpressionParser expressionParser;
	private final ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider;
	private final ExpressionDependencies expressionDependencies;

	/**
	 * Creates a new {@link StringBasedR2dbcQuery} for the given {@link StringBasedR2dbcQuery}, {@link DatabaseClient},
	 * {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(R2dbcQueryMethod queryMethod, DatabaseClient databaseClient, R2dbcConverter converter,
			ReactiveDataAccessStrategy dataAccessStrategy,
			ExpressionParser expressionParser, ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {
		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, databaseClient, converter, dataAccessStrategy,
				expressionParser,
				evaluationContextProvider);
	}

	/**
	 * Create a new {@link StringBasedR2dbcQuery} for the given {@code query}, {@link R2dbcQueryMethod},
	 * {@link DatabaseClient}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(String query, R2dbcQueryMethod method, DatabaseClient databaseClient,
			R2dbcConverter converter, ReactiveDataAccessStrategy dataAccessStrategy, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, databaseClient, converter);
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;

		Assert.hasText(query, "Query must not be empty");

		this.expressionQuery = ExpressionQuery.create(query);
		this.binder = new ExpressionEvaluatingParameterBinder(expressionQuery, dataAccessStrategy);
		this.expressionDependencies = createExpressionDependencies();
	}

	private ExpressionDependencies createExpressionDependencies() {

		if (expressionQuery.getBindings().isEmpty()) {
			return ExpressionDependencies.none();
		}

		List<ExpressionDependencies> dependencies = new ArrayList<>();

		for (ExpressionQuery.ParameterBinding binding : expressionQuery.getBindings()) {
			dependencies.add(ExpressionDependencies.discover(expressionParser.parseExpression(binding.getExpression())));
		}

		return ExpressionDependencies.merged(dependencies);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.repository.query.AbstractR2dbcQuery#isModifyingQuery()
	 */
	@Override
	protected boolean isModifyingQuery() {
		return getQueryMethod().isModifyingQuery();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.repository.query.AbstractR2dbcQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.repository.query.AbstractR2dbcQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.repository.query.AbstractR2dbcQuery#createQuery(org.springframework.data.relational.repository.query.RelationalParameterAccessor)
	 */
	@Override
	protected Mono<BindableQuery> createQuery(RelationalParameterAccessor accessor) {

		return getSpelEvaluator(accessor).map(evaluator -> new BindableQuery() {

			@Override
			public DatabaseClient.GenericExecuteSpec bind(DatabaseClient.GenericExecuteSpec bindSpec) {
				return binder.bind(bindSpec, accessor, evaluator);
			}

			@Override
			public String get() {
				return expressionQuery.getQuery();
			}
		});
	}

	@Override
	Class<?> resolveResultType(ResultProcessor resultProcessor) {

		Class<?> returnedType = resultProcessor.getReturnedType().getReturnedType();
		return !returnedType.isInterface() ? returnedType : super.resolveResultType(resultProcessor);
	}

	private Mono<R2dbcSpELExpressionEvaluator> getSpelEvaluator(RelationalParameterAccessor accessor) {

		return evaluationContextProvider
				.getEvaluationContextLater(getQueryMethod().getParameters(), accessor.getValues(), expressionDependencies)
				.<R2dbcSpELExpressionEvaluator> map(
						context -> new DefaultR2dbcSpELExpressionEvaluator(expressionParser, context))
				.defaultIfEmpty(DefaultR2dbcSpELExpressionEvaluator.unsupported());
	}
}
