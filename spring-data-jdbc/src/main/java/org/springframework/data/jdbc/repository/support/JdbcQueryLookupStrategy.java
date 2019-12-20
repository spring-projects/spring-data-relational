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
package org.springframework.data.jdbc.repository.support;

import lombok.RequiredArgsConstructor;

import java.lang.reflect.Method;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.EntityRowMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * {@link QueryLookupStrategy} for JDBC repositories. Currently only supports annotated queries.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Moises Cisneros
 */
@RequiredArgsConstructor
class JdbcQueryLookupStrategy implements QueryLookupStrategy {

	private final ApplicationEventPublisher publisher;
	private final EntityCallbacks callbacks;
	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final QueryMappingConfiguration queryMappingConfiguration;
	private final NamedParameterJdbcOperations operations;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
	 */
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, NamedQueries namedQueries) {

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, repositoryMetadata, projectionFactory, namedQueries);

		RowMapper<?> mapper = queryMethod.isModifyingQuery() ? null : createMapper(queryMethod);

		return new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, mapper, converter);
	}

	private RowMapper<?> createMapper(JdbcQueryMethod queryMethod) {

		Class<?> returnedObjectType = queryMethod.getReturnedObjectType();

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(returnedObjectType);

		if (persistentEntity == null) {
			return SingleColumnRowMapper.newInstance(returnedObjectType, converter.getConversionService());
		}

		return determineDefaultMapper(queryMethod);
	}

	private RowMapper<?> determineDefaultMapper(JdbcQueryMethod queryMethod) {

		Class<?> domainType = queryMethod.getReturnedObjectType();
		RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(domainType);

		if (configuredQueryMapper != null)
			return configuredQueryMapper;

		EntityRowMapper<?> defaultEntityRowMapper = new EntityRowMapper<>( //
				context.getRequiredPersistentEntity(domainType), //
				converter //
		);

		return defaultEntityRowMapper;
	}
}
