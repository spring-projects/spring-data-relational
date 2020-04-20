/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.Optional;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Creates repository implementation based on JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class JdbcRepositoryFactory extends RepositoryFactorySupport {

	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final ApplicationEventPublisher publisher;
	private final DataAccessStrategy accessStrategy;
	private final NamedParameterJdbcOperations operations;
	private final Dialect dialect;

	private QueryMappingConfiguration queryMappingConfiguration = QueryMappingConfiguration.EMPTY;
	private EntityCallbacks entityCallbacks;

	/**
	 * Creates a new {@link JdbcRepositoryFactory} for the given {@link DataAccessStrategy},
	 * {@link RelationalMappingContext} and {@link ApplicationEventPublisher}.
	 *
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param publisher must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public JdbcRepositoryFactory(DataAccessStrategy dataAccessStrategy, RelationalMappingContext context,
			JdbcConverter converter, Dialect dialect, ApplicationEventPublisher publisher,
			NamedParameterJdbcOperations operations) {

		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dialect, "Dialect must not be null!");
		Assert.notNull(publisher, "ApplicationEventPublisher must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.converter = converter;
		this.dialect = dialect;
		this.accessStrategy = dataAccessStrategy;
		this.operations = operations;
	}

	/**
	 * @param queryMappingConfiguration must not be {@literal null} consider {@link QueryMappingConfiguration#EMPTY}
	 *          instead.
	 */
	public void setQueryMappingConfiguration(QueryMappingConfiguration queryMappingConfiguration) {

		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null!");

		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> aClass) {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(aClass);

		return (EntityInformation<T, ID>) new PersistentEntityInformation<>(entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	protected Object getTargetRepository(RepositoryInformation repositoryInformation) {

		JdbcAggregateTemplate template = new JdbcAggregateTemplate(publisher, context, converter, accessStrategy);

		SimpleJdbcRepository<?, Object> repository = new SimpleJdbcRepository<>(template,
				context.getRequiredPersistentEntity(repositoryInformation.getDomainType()));

		if (entityCallbacks != null) {
			template.setEntityCallbacks(entityCallbacks);
		}

		return repository;
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
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable QueryLookupStrategy.Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		return Optional.of(new JdbcQueryLookupStrategy(publisher, entityCallbacks, context, converter, dialect,
				queryMappingConfiguration, operations));
	}

	/**
	 * @param entityCallbacks
	 * @since 1.1
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {
		this.entityCallbacks = entityCallbacks;
	}
}
