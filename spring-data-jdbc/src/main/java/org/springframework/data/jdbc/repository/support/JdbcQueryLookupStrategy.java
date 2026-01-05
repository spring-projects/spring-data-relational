/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.PartTreeJdbcQuery;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.jdbc.repository.query.StringBasedJdbcQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.repository.support.RelationalQueryLookupStrategy;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.Assert;

/**
 * Abstract {@link QueryLookupStrategy} for JDBC repositories.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Moises Cisneros
 * @author Hebert Coelho
 * @author Diego Krupitza
 * @author Christopher Klein
 * @author Mikhail Polivakha
 */
abstract class JdbcQueryLookupStrategy extends RelationalQueryLookupStrategy {

	private static final Log LOG = LogFactory.getLog(JdbcQueryLookupStrategy.class);

	final JdbcAggregateOperations operations;
	final RowMapperFactory rowMapperFactory;
	final ValueExpressionDelegate delegate;

	JdbcQueryLookupStrategy(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
			ValueExpressionDelegate delegate) {

		super(operations.getConverter().getMappingContext(), operations.getDataAccessStrategy().getDialect());

		Assert.notNull(rowMapperFactory, "RowMapperFactory must not be null");
		Assert.notNull(delegate, "ValueExpressionDelegate must not be null");

		this.rowMapperFactory = rowMapperFactory;
		this.operations = operations;
		this.delegate = delegate;
	}

	/**
	 * {@link QueryLookupStrategy} to create a query from the method name.
	 *
	 * @author Diego Krupitza
	 * @since 2.4
	 */
	static class CreateQueryLookupStrategy extends JdbcQueryLookupStrategy {

		CreateQueryLookupStrategy(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
				ValueExpressionDelegate delegate) {

			super(operations, rowMapperFactory, delegate);
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
				ProjectionFactory projectionFactory, NamedQueries namedQueries) {

			JdbcQueryMethod queryMethod = getJdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries);

			return new PartTreeJdbcQuery(queryMethod, operations, rowMapperFactory);
		}
	}

	/**
	 * {@link QueryLookupStrategy} that tries to detect a declared query declared via
	 * {@link org.springframework.data.jdbc.repository.query.Query} annotation followed by a JPA named query lookup.
	 *
	 * @author Diego Krupitza
	 * @since 2.4
	 */
	static class DeclaredQueryLookupStrategy extends JdbcQueryLookupStrategy {

		DeclaredQueryLookupStrategy(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
				ValueExpressionDelegate delegate) {
			super(operations, rowMapperFactory, delegate);
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
				ProjectionFactory projectionFactory, NamedQueries namedQueries) {

			JdbcQueryMethod queryMethod = getJdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries);

			if (namedQueries.hasQuery(queryMethod.getNamedQueryName()) || queryMethod.hasAnnotatedQuery()) {

				if (queryMethod.hasAnnotatedQuery() && queryMethod.hasAnnotatedQueryName()) {
					LOG.warn(String.format(
							"Query method %s is annotated with both, a query and a query name; Using the declared query", method));
				}

				String queryString = evaluateTableExpressions(repositoryMetadata, queryMethod.getRequiredQuery());

				return new StringBasedJdbcQuery(queryString, queryMethod, operations, rowMapperFactory, delegate);
			}

			throw new IllegalStateException(
					String.format("Did neither find a NamedQuery nor an annotated query for method %s", method));
		}
	}

	/**
	 * {@link QueryLookupStrategy} to try to detect a declared query first (
	 * {@link org.springframework.data.jdbc.repository.query.Query}, JDBC named query). In case none is found we fall back
	 * on query creation.
	 *
	 * @author Diego Krupitza
	 * @since 2.4
	 */
	static class CreateIfNotFoundQueryLookupStrategy extends JdbcQueryLookupStrategy {

		private final DeclaredQueryLookupStrategy lookupStrategy;
		private final CreateQueryLookupStrategy createStrategy;

		/**
		 * Creates a new {@link CreateIfNotFoundQueryLookupStrategy}.
		 *
		 * @param createStrategy must not be {@literal null}.
		 * @param lookupStrategy must not be {@literal null}.
		 */
		CreateIfNotFoundQueryLookupStrategy(JdbcAggregateOperations operations, RowMapperFactory rowMapperFactory,
				CreateQueryLookupStrategy createStrategy, DeclaredQueryLookupStrategy lookupStrategy,
				ValueExpressionDelegate delegate) {

			super(operations, rowMapperFactory, delegate);

			Assert.notNull(createStrategy, "CreateQueryLookupStrategy must not be null");
			Assert.notNull(lookupStrategy, "DeclaredQueryLookupStrategy must not be null");

			this.createStrategy = createStrategy;
			this.lookupStrategy = lookupStrategy;
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
				ProjectionFactory projectionFactory, NamedQueries namedQueries) {

			try {
				return lookupStrategy.resolveQuery(method, repositoryMetadata, projectionFactory, namedQueries);
			} catch (IllegalStateException e) {
				return createStrategy.resolveQuery(method, repositoryMetadata, projectionFactory, namedQueries);
			}
		}
	}

	/**
	 * Creates a {@link JdbcQueryMethod} based on the parameters
	 */
	JdbcQueryMethod getJdbcQueryMethod(Method method, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, NamedQueries namedQueries) {
		return new JdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries, getMappingContext());
	}

	/**
	 * Creates a {@link QueryLookupStrategy} based on the provided
	 * {@link org.springframework.data.repository.query.QueryLookupStrategy.Key}.
	 *
	 * @param key the key that decides what {@link QueryLookupStrategy} should be used.
	 * @param operations must not be {@literal null}
	 */
	public static QueryLookupStrategy create(@Nullable Key key, JdbcAggregateOperations operations,
			RowMapperFactory rowMapperFactory, ValueExpressionDelegate delegate) {

		Assert.notNull(operations, "JdbcAggregateOperations must not be null");
		Assert.notNull(rowMapperFactory, "RowMapperFactory must not be null");
		Assert.notNull(delegate, "ValueExpressionDelegate must not be null");

		CreateQueryLookupStrategy createQueryLookupStrategy = new CreateQueryLookupStrategy(operations, rowMapperFactory,
				delegate);

		DeclaredQueryLookupStrategy declaredQueryLookupStrategy = new DeclaredQueryLookupStrategy(operations,
				rowMapperFactory, delegate);

		Key keyToUse = key != null ? key : Key.CREATE_IF_NOT_FOUND;

		LOG.debug(String.format("Using the queryLookupStrategy %s", keyToUse));

		return switch (keyToUse) {
			case CREATE -> createQueryLookupStrategy;
			case USE_DECLARED_QUERY -> declaredQueryLookupStrategy;
			case CREATE_IF_NOT_FOUND -> new CreateIfNotFoundQueryLookupStrategy(operations, rowMapperFactory,
					createQueryLookupStrategy, declaredQueryLookupStrategy, delegate);
		};
	}

}
