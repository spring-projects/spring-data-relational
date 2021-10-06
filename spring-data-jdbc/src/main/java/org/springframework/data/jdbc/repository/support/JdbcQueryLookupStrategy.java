/*
 * Copyright 2018-2021 the original author or authors.
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
import org.springframework.data.relational.core.mapping.event.AfterLoadCallback;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link QueryLookupStrategy} for JDBC repositories.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Moises Cisneros
 * @author Hebert Coelho
 */
class JdbcQueryLookupStrategy implements QueryLookupStrategy {

	private static final Log LOG = LogFactory.getLog(JdbcQueryLookupStrategy.class);

	private final ApplicationEventPublisher publisher;
	private final @Nullable EntityCallbacks callbacks;
	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final Dialect dialect;
	private final QueryMappingConfiguration queryMappingConfiguration;
	private final NamedParameterJdbcOperations operations;
	private final BeanFactory beanfactory;

	JdbcQueryLookupStrategy(ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks,
			RelationalMappingContext context, JdbcConverter converter, Dialect dialect,
			QueryMappingConfiguration queryMappingConfiguration, NamedParameterJdbcOperations operations,
		    BeanFactory beanfactory) {

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null");
		Assert.notNull(context, "RelationalMappingContextPublisher must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");

		this.publisher = publisher;
		this.callbacks = callbacks;
		this.context = context;
		this.converter = converter;
		this.dialect = dialect;
		this.queryMappingConfiguration = queryMappingConfiguration;
		this.operations = operations;
		this.beanfactory = beanfactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
	 */
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, NamedQueries namedQueries) {

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries,
				context);

		try {
			if (namedQueries.hasQuery(queryMethod.getNamedQueryName()) || queryMethod.hasAnnotatedQuery()) {

				if (queryMethod.hasAnnotatedQuery() && queryMethod.hasAnnotatedQueryName()) {
					LOG.warn(String.format(
							"Query method %s is annotated with both, a query and a query name. Using the declared query.", method));
				}

				StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, this::createMapper, converter);
				query.setBeanFactory(beanfactory);
				return query;
			} else {
				return new PartTreeJdbcQuery(context, queryMethod, dialect, converter, operations, this::createMapper);
			}
		} catch (Exception e) {
			throw QueryCreationException.create(queryMethod, e);
		}
	}

	@SuppressWarnings("unchecked")
	private RowMapper<Object> createMapper(Class<?> returnedObjectType) {

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(returnedObjectType);

		if (persistentEntity == null) {
			return (RowMapper<Object>) SingleColumnRowMapper.newInstance(returnedObjectType, converter.getConversionService());
		}

		return (RowMapper<Object>) determineDefaultMapper(returnedObjectType);
	}

	private RowMapper<?> determineDefaultMapper(Class<?> returnedObjectType) {

		RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(returnedObjectType);

		if (configuredQueryMapper != null)
			return configuredQueryMapper;

		EntityRowMapper<?> defaultEntityRowMapper = new EntityRowMapper<>( //
				context.getRequiredPersistentEntity(returnedObjectType), //
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

				publisher.publishEvent(new AfterLoadEvent<>(entity));
				publisher.publishEvent(new AfterConvertEvent<>(entity));

				if (callbacks != null) {
					entity = callbacks.callback(AfterLoadCallback.class, entity);
					return callbacks.callback(AfterConvertCallback.class, entity);
				}
			}

			return entity;
		}
	}
}
