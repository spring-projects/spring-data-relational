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

import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
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

	/**
	 * Creates a new {@link StringBasedR2dbcQuery} for the given {@link StringBasedR2dbcQuery}, {@link DatabaseClient},
	 * {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(R2dbcQueryMethod queryMethod, DatabaseClient databaseClient, R2dbcConverter converter,
			SpelExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		this(queryMethod.getRequiredAnnotatedQuery(), queryMethod, databaseClient, converter, expressionParser,
				evaluationContextProvider);
	}

	/**
	 * Create a new {@link StringBasedR2dbcQuery} for the given {@code query}, {@link R2dbcQueryMethod},
	 * {@link DatabaseClient}, {@link SpelExpressionParser}, and {@link QueryMethodEvaluationContextProvider}.
	 *
	 * @param method must not be {@literal null}.
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedR2dbcQuery(String query, R2dbcQueryMethod method, DatabaseClient databaseClient,
			R2dbcConverter converter, SpelExpressionParser expressionParser,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, databaseClient, converter);

		Assert.hasText(query, "Query must not be empty");

		this.expressionQuery = ExpressionQuery.create(query);
		this.binder = new ExpressionEvaluatingParameterBinder(expressionParser, evaluationContextProvider, expressionQuery);
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
	 * @see org.springframework.data.r2dbc.repository.query.AbstractR2dbcQuery#createQuery(org.springframework.data.relational.repository.query.RelationalParameterAccessor)
	 */
	@Override
	protected BindableQuery createQuery(RelationalParameterAccessor accessor) {

		return new BindableQuery() {

			@Override
			public DatabaseClient.GenericExecuteSpec bind(DatabaseClient.GenericExecuteSpec bindSpec) {
				return binder.bind(bindSpec, accessor);
			}

			@Override
			public String get() {
				return expressionQuery.getQuery();
			}
		};
	}
}
