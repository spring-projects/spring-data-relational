/*
 * Copyright 2017-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Creates repository implementation based on JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Hebert Coelho
 * @author Diego Krupitza
 * @author Christopher Klein
 * @author Marcin Grzejszczak
 * @author Tomohiko Ozawa
 */
public class JdbcRepositoryFactory extends RepositoryFactorySupport implements ApplicationEventPublisherAware {

	private final JdbcAggregateOperations operations;
	private final NamedParameterJdbcOperations jdbcOperations;

	private EntityCallbacks entityCallbacks = EntityCallbacks.create();
	private ApplicationEventPublisher publisher = event -> {};
	private @Nullable BeanFactory beanFactory;
	private QueryMappingConfiguration queryMappingConfiguration = QueryMappingConfiguration.EMPTY;

	/**
	 * Creates a new {@link JdbcRepositoryFactory} for the given {@link JdbcAggregateOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 * @since 4.0
	 */
	public JdbcRepositoryFactory(JdbcAggregateOperations operations) {

		Assert.notNull(operations, "JdbcAggregateOperations must not be null");

		this.operations = operations;
		this.jdbcOperations = operations.getDataAccessStrategy().getJdbcOperations();
	}

	/**
	 * Creates a new {@link JdbcRepositoryFactory} for the given {@link DataAccessStrategy},
	 * {@link RelationalMappingContext} and {@link ApplicationEventPublisher}.
	 *
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param publisher must not be {@literal null}.
	 * @param jdbcOperations must not be {@literal null}.
	 */
	public JdbcRepositoryFactory(DataAccessStrategy dataAccessStrategy, RelationalMappingContext context,
			JdbcConverter converter, Dialect dialect, ApplicationEventPublisher publisher,
			NamedParameterJdbcOperations jdbcOperations) {

		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null");
		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(converter, "RelationalConverter must not be null");
		Assert.notNull(publisher, "ApplicationEventPublisher must not be null");
		Assert.notNull(jdbcOperations, "NamedParameterJdbcOperations must not be null");

		this.operations = new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		this.jdbcOperations = jdbcOperations;
		this.publisher = publisher;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null");

		this.publisher = publisher;
	}

	/**
	 * @param entityCallbacks
	 * @since 1.1
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null");

		this.entityCallbacks = entityCallbacks;
	}

	/**
	 * @param beanFactory the {@link BeanFactory} used for looking up {@link org.springframework.jdbc.core.RowMapper} and
	 *          {@link org.springframework.jdbc.core.ResultSetExtractor} beans.
	 */
	public void setBeanFactory(@Nullable BeanFactory beanFactory) {

		this.beanFactory = beanFactory;

		if (entityCallbacks == null && beanFactory != null) {
			setEntityCallbacks(EntityCallbacks.create(beanFactory));
		}
	}

	/**
	 * @param queryMappingConfiguration must not be {@literal null} consider {@link QueryMappingConfiguration#EMPTY}
	 *          instead.
	 */
	public void setQueryMappingConfiguration(QueryMappingConfiguration queryMappingConfiguration) {

		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");

		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	@Override
	public RelationalEntityInformation<?, ?> getEntityInformation(RepositoryMetadata metadata) {

		RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(metadata.getDomainType());

		return new MappingRelationalEntityInformation<>(entity);
	}

	private RelationalMappingContext getMappingContext() {
		return operations.getConverter().getMappingContext();
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation repositoryInformation) {

		RelationalPersistentEntity<?> persistentEntity = getMappingContext()
				.getRequiredPersistentEntity(repositoryInformation.getDomainType());

		return getTargetRepositoryViaReflection(repositoryInformation, operations, persistentEntity,
				operations.getConverter());
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata repositoryMetadata) {
		return SimpleJdbcRepository.class;
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(QueryLookupStrategy.@Nullable Key key,
			ValueExpressionDelegate valueExpressionDelegate) {

		DataAccessStrategy strategy = operations.getDataAccessStrategy();
		JdbcConverter converter = operations.getConverter();

		return Optional.of(JdbcQueryLookupStrategy.create(key, publisher, entityCallbacks, converter, strategy.getDialect(),
				queryMappingConfiguration, jdbcOperations, beanFactory,
				new CachingValueExpressionDelegate(valueExpressionDelegate)));
	}

}
