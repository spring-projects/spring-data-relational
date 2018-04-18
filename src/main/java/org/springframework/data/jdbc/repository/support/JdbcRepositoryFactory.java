/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.support;

import java.util.Optional;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Creates repository implementation based on JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @since 1.0
 */
public class JdbcRepositoryFactory extends RepositoryFactorySupport {

	private final JdbcMappingContext context;
	private final ApplicationEventPublisher publisher;
	private final DataAccessStrategy accessStrategy;
	private final NamedParameterJdbcOperations operations;

	private RowMapperMap rowMapperMap = RowMapperMap.EMPTY;
	private EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Creates a new {@link JdbcRepositoryFactory} for the given {@link DataAccessStrategy}, {@link JdbcMappingContext}
	 * and {@link ApplicationEventPublisher}.
	 * 
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param publisher must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public JdbcRepositoryFactory(DataAccessStrategy dataAccessStrategy, JdbcMappingContext context,
			ApplicationEventPublisher publisher, NamedParameterJdbcOperations operations) {

		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");
		Assert.notNull(context, "JdbcMappingContext must not be null!");
		Assert.notNull(publisher, "ApplicationEventPublisher must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;
		this.operations = operations;
	}

	/**
	 * @param rowMapperMap must not be {@literal null} consider {@link RowMapperMap#EMPTY} instead.
	 */
	public void setRowMapperMap(RowMapperMap rowMapperMap) {

		Assert.notNull(rowMapperMap, "RowMapperMap must not be null!");

		this.rowMapperMap = rowMapperMap;
	}

	/**
	 * Set the {@link EntityInstantiators} used for instantiating entity instances.
	 * 
	 * @param instantiators Must not be {@code null}.
	 */
	public void setEntityInstantiators(EntityInstantiators instantiators) {

		Assert.notNull(instantiators, "EntityInstantiators must not be null.");

		this.instantiators = instantiators;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> aClass) {

		JdbcPersistentEntity<?> entity = context.getPersistentEntity(aClass);

		return (EntityInformation<T, ID>) new PersistentEntityInformation<>(entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	protected Object getTargetRepository(RepositoryInformation repositoryInformation) {

		JdbcAggregateTemplate template = new JdbcAggregateTemplate(publisher, context, accessStrategy);

		return new SimpleJdbcRepository<>(template, context.getPersistentEntity(repositoryInformation.getDomainType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata repositoryMetadata) {
		return SimpleJdbcRepository.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getQueryLookupStrategy(org.springframework.data.repository.query.QueryLookupStrategy.Key, org.springframework.data.repository.query.EvaluationContextProvider)
	 */
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(QueryLookupStrategy.Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		if (key != null //
				&& key != QueryLookupStrategy.Key.USE_DECLARED_QUERY //
				&& key != QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND //
		) {
			throw new IllegalArgumentException(String.format("Unsupported query lookup strategy %s!", key));
		}

		return Optional.of(new JdbcQueryLookupStrategy(context, instantiators, accessStrategy, rowMapperMap, operations));
	}
}
