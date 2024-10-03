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
package org.springframework.data.jdbc.repository.support;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.EntityRowMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.query.AbstractJdbcQuery;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.PartTreeJdbcQuery;
import org.springframework.data.jdbc.repository.query.StringBasedJdbcQuery;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.AfterConvertEvent;
import org.springframework.data.relational.repository.support.RelationalQueryLookupStrategy;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
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
 */
abstract class JdbcQueryLookupStrategy extends RelationalQueryLookupStrategy {

	private static final Log LOG = LogFactory.getLog(JdbcQueryLookupStrategy.class);

	private final ApplicationEventPublisher publisher;
	private final RelationalMappingContext context;
	private final @Nullable EntityCallbacks callbacks;
	private final JdbcConverter converter;
	private final QueryMappingConfiguration queryMappingConfiguration;
	private final NamedParameterJdbcOperations operations;
	protected final ValueExpressionDelegate delegate;

	JdbcQueryLookupStrategy(ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks,
			RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
			QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
			ValueExpressionDelegate delegate) {

		super(context, dialect);

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");
		Assert.notNull(delegate, "ValueExpressionDelegate must not be null");

		this.context = context;
		this.publisher = publisher;
		this.callbacks = callbacks;
		this.converter = converter;
		this.queryMappingConfiguration = queryMappingConfiguration;
		this.operations = operations;
		this.delegate = delegate;
	}

	public RelationalMappingContext getMappingContext() {
		return context;
	}

	/**
	 * {@link QueryLookupStrategy} to create a query from the method name.
	 *
	 * @author Diego Krupitza
	 * @since 2.4
	 */
	static class CreateQueryLookupStrategy extends JdbcQueryLookupStrategy {

		CreateQueryLookupStrategy(ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks,
				RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
				QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
				ValueExpressionDelegate delegate) {

			super(publisher, callbacks, context, converter, dialect, queryMappingConfiguration, operations,
					delegate);
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
				ProjectionFactory projectionFactory, NamedQueries namedQueries) {

			JdbcQueryMethod queryMethod = getJdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries);

			return new PartTreeJdbcQuery(getMappingContext(), queryMethod, getDialect(), getConverter(), getOperations(),
					this::createMapper);
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

		private final AbstractJdbcQuery.RowMapperFactory rowMapperFactory;

		DeclaredQueryLookupStrategy(ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks,
				RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
				QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
				@Nullable BeanFactory beanfactory, ValueExpressionDelegate delegate) {
			super(publisher, callbacks, context, converter, dialect, queryMappingConfiguration, operations,
					delegate);

			this.rowMapperFactory = new BeanFactoryRowMapperFactory(beanfactory);
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

				return new StringBasedJdbcQuery(queryString, queryMethod, getOperations(), rowMapperFactory, getConverter(),
						delegate);
			}

			throw new IllegalStateException(
					String.format("Did neither find a NamedQuery nor an annotated query for method %s", method));
		}

		@SuppressWarnings("unchecked")
		private class BeanFactoryRowMapperFactory implements AbstractJdbcQuery.RowMapperFactory {

			private final @Nullable BeanFactory beanFactory;

			BeanFactoryRowMapperFactory(@Nullable BeanFactory beanFactory) {
				this.beanFactory = beanFactory;
			}

			@Override
			public RowMapper<Object> create(Class<?> result) {
				return createMapper(result);
			}

			@Override
			public RowMapper<Object> getRowMapper(String reference) {

				if (beanFactory == null) {
					throw new IllegalStateException(
							"Cannot resolve RowMapper bean reference '" + reference + "'; BeanFactory is not configured.");
				}

				return beanFactory.getBean(reference, RowMapper.class);
			}

			@Override
			public ResultSetExtractor<Object> getResultSetExtractor(String reference) {

				if (beanFactory == null) {
					throw new IllegalStateException(
							"Cannot resolve ResultSetExtractor bean reference '" + reference + "'; BeanFactory is not configured.");
				}

				return beanFactory.getBean(reference, ResultSetExtractor.class);
			}
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
		CreateIfNotFoundQueryLookupStrategy(ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks,
				RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
				QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
				CreateQueryLookupStrategy createStrategy,
				DeclaredQueryLookupStrategy lookupStrategy, ValueExpressionDelegate delegate) {

			super(publisher, callbacks, context, converter, dialect, queryMappingConfiguration, operations,
					delegate);

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
	 * @param key the key that decides what {@link QueryLookupStrategy} shozld be used.
	 * @param publisher must not be {@literal null}
	 * @param callbacks may be {@literal null}
	 * @param context must not be {@literal null}
	 * @param converter must not be {@literal null}
	 * @param dialect must not be {@literal null}
	 * @param queryMappingConfiguration must not be {@literal null}
	 * @param operations must not be {@literal null}
	 * @param beanFactory may be {@literal null}
	 */
	public static QueryLookupStrategy create(@Nullable Key key, ApplicationEventPublisher publisher,
			@Nullable EntityCallbacks callbacks, RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
			QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
			@Nullable BeanFactory beanFactory, ValueExpressionDelegate delegate) {
		Assert.notNull(publisher, "ApplicationEventPublisher must not be null");
		Assert.notNull(context, "RelationalMappingContextPublisher must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");
		Assert.notNull(delegate, "ValueExpressionDelegate must not be null");

		CreateQueryLookupStrategy createQueryLookupStrategy = new CreateQueryLookupStrategy(publisher, callbacks, context,
				converter, dialect, queryMappingConfiguration, operations, delegate);

		DeclaredQueryLookupStrategy declaredQueryLookupStrategy = new DeclaredQueryLookupStrategy(publisher, callbacks,
				context, converter, dialect, queryMappingConfiguration, operations, beanFactory, delegate);

		Key keyToUse = key != null ? key : Key.CREATE_IF_NOT_FOUND;

		LOG.debug(String.format("Using the queryLookupStrategy %s", keyToUse));

		switch (keyToUse) {
			case CREATE:
				return createQueryLookupStrategy;
			case USE_DECLARED_QUERY:
				return declaredQueryLookupStrategy;
			case CREATE_IF_NOT_FOUND:
				return new CreateIfNotFoundQueryLookupStrategy(publisher, callbacks, context, converter, dialect,
						queryMappingConfiguration, operations, createQueryLookupStrategy, declaredQueryLookupStrategy,
						delegate);
			default:
				throw new IllegalArgumentException(String.format("Unsupported query lookup strategy %s", key));
		}
	}

	JdbcConverter getConverter() {
		return converter;
	}

	NamedParameterJdbcOperations getOperations() {
		return operations;
	}

	@SuppressWarnings("unchecked")
	RowMapper<Object> createMapper(Class<?> returnedObjectType) {

		RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(returnedObjectType);

		if (persistentEntity == null) {
			return (RowMapper<Object>) SingleColumnRowMapper.newInstance(returnedObjectType,
					converter.getConversionService());
		}

		return (RowMapper<Object>) determineDefaultMapper(returnedObjectType);
	}

	private RowMapper<?> determineDefaultMapper(Class<?> returnedObjectType) {

		RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(returnedObjectType);

		if (configuredQueryMapper != null)
			return configuredQueryMapper;

		EntityRowMapper<?> defaultEntityRowMapper = new EntityRowMapper<>( //
				getMappingContext().getRequiredPersistentEntity(returnedObjectType), //
				converter //
		);

		return new PostProcessingRowMapper<>(defaultEntityRowMapper);
	}

	class PostProcessingRowMapper<T> implements RowMapper<T> {

		private final RowMapper<T> delegate;

		PostProcessingRowMapper(RowMapper<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {

			T entity = delegate.mapRow(rs, rowNum);

			if (entity != null) {

				publisher.publishEvent(new AfterConvertEvent<>(entity));

				if (callbacks != null) {
					return callbacks.callback(AfterConvertCallback.class, entity);
				}
			}

			return entity;
		}
	}
}
