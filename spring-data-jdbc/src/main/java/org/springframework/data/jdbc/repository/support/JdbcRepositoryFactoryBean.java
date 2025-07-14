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

import java.io.Serializable;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.InsertStrategyFactory;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.convert.SqlParametersFactory;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Special adapter for Springs {@link org.springframework.beans.factory.FactoryBean} interface to allow easy setup of
 * repository factories via Spring configuration.
 * <p>
 * A partially populated factory bean can use {@link BeanFactory} to resolve missing dependencies, specifically:
 * <ul>
 * <li>The {@link org.springframework.data.mapping.context.MappingContext} is being derived from
 * {@link RelationalMappingContext} if the {@code mappingContext} was not set.</li>
 * <li>The {@link NamedParameterJdbcOperations} is looked up from a {@link BeanFactory} if {@code jdbcOperations} was
 * not set.</li>
 * <li>The {@link QueryMappingConfiguration} is looked up from a {@link BeanFactory} if
 * {@code queryMappingConfiguration} was not set. If the {@link BeanFactory} was not set, defaults to
 * {@link QueryMappingConfiguration#EMPTY}.</li>
 * <li>The {@link DataAccessStrategy} is looked up from a {@link BeanFactory} if {@code dataAccessStrategy} was not set.
 * If the {@link BeanFactory} was not set, then it is created using {@link Dialect}.</li>
 * </ul>
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Hebert Coelho
 * @author Chirag Tailor
 * @author Mikhail Polivakha
 * @author Sergey Korotaev
 */
public class JdbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends TransactionalRepositoryFactoryBeanSupport<T, S, ID> implements ApplicationEventPublisherAware {

	private @Nullable ApplicationEventPublisher publisher;
	private @Nullable BeanFactory beanFactory;
	private @Nullable RelationalMappingContext mappingContext;
	private @Nullable JdbcConverter converter;
	private @Nullable DataAccessStrategy dataAccessStrategy;
	private @Nullable QueryMappingConfiguration queryMappingConfiguration;
	private @Nullable NamedParameterJdbcOperations operations;
	private EntityCallbacks entityCallbacks = EntityCallbacks.create();
	private @Nullable Dialect dialect;

	/**
	 * Creates a new {@link JdbcRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public JdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {

		super.setApplicationEventPublisher(publisher);

		this.publisher = publisher;
	}

	/**
	 * Creates the actual {@link RepositoryFactorySupport} instance.
	 */
	@Override
	protected RepositoryFactorySupport doCreateRepositoryFactory() {

		Assert.state(this.dataAccessStrategy != null, "DataAccessStrategy is required and must not be null");
		Assert.state(this.mappingContext != null, "MappingContext is required and must not be null");
		Assert.state(this.converter != null, "RelationalConverter is required and must not be null");
		Assert.state(this.dialect != null, "Dialect is required and must not be null");
		Assert.state(this.publisher != null, "ApplicationEventPublisher is required and must not be null");
		Assert.state(this.operations != null, "NamedParameterJdbcOperations is required and must not be null");
		Assert.state(this.queryMappingConfiguration != null, "RelationalConverter is required and must not be null");

		JdbcRepositoryFactory jdbcRepositoryFactory = new JdbcRepositoryFactory(dataAccessStrategy, mappingContext,
				converter, dialect, publisher, operations);
		jdbcRepositoryFactory.setQueryMappingConfiguration(queryMappingConfiguration);
		jdbcRepositoryFactory.setEntityCallbacks(entityCallbacks);
		jdbcRepositoryFactory.setBeanFactory(beanFactory);

		return jdbcRepositoryFactory;
	}

	public void setMappingContext(RelationalMappingContext mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null");

		super.setMappingContext(mappingContext);
		this.mappingContext = mappingContext;
	}

	public void setDialect(Dialect dialect) {

		Assert.notNull(dialect, "Dialect must not be null");

		this.dialect = dialect;
	}

	/**
	 * @param dataAccessStrategy can be {@literal null}.
	 */
	public void setDataAccessStrategy(DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null");

		this.dataAccessStrategy = dataAccessStrategy;
	}

	/**
	 * @param queryMappingConfiguration can be {@literal null}. {@link #afterPropertiesSet()} defaults to
	 *          {@link QueryMappingConfiguration#EMPTY} if {@literal null}.
	 */
	public void setQueryMappingConfiguration(QueryMappingConfiguration queryMappingConfiguration) {

		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");

		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	public void setJdbcOperations(NamedParameterJdbcOperations operations) {

		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");

		this.operations = operations;
	}

	public void setConverter(JdbcConverter converter) {

		Assert.notNull(converter, "JdbcConverter must not be null");

		this.converter = converter;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {

		super.setBeanFactory(beanFactory);

		this.entityCallbacks = EntityCallbacks.create(beanFactory);
		this.beanFactory = beanFactory;
	}

	@Override
	public void afterPropertiesSet() {

		Assert.state(this.converter != null, "RelationalConverter is required and must not be null");

		if (this.mappingContext == null) {
			this.mappingContext = this.converter.getMappingContext();
		}

		if (this.operations == null) {

			Assert.state(this.beanFactory != null, "If no JdbcOperations are set a BeanFactory must be available");
			this.operations = this.beanFactory.getBean(NamedParameterJdbcOperations.class);
		}

		if (this.queryMappingConfiguration == null) {

			if (this.beanFactory == null) {
				this.queryMappingConfiguration = QueryMappingConfiguration.EMPTY;
			} else {

				this.queryMappingConfiguration = beanFactory.getBeanProvider(QueryMappingConfiguration.class)
						.getIfAvailable(() -> QueryMappingConfiguration.EMPTY);
			}
		}

		if (this.dataAccessStrategy == null && this.beanFactory != null) {
			this.dataAccessStrategy = this.beanFactory.getBeanProvider(DataAccessStrategy.class).getIfAvailable();
		}

		if (this.dataAccessStrategy == null) {

			Assert.state(this.dialect != null, "Dialect is required and must not be null");

			DataAccessStrategyFactory factory = getDataAccessStrategyFactory(this.mappingContext, this.converter,
					this.dialect, this.operations, this.queryMappingConfiguration);

			this.dataAccessStrategy = factory.create();
		}

		super.afterPropertiesSet();
	}

	private static DataAccessStrategyFactory getDataAccessStrategyFactory(RelationalMappingContext mappingContext,
			JdbcConverter converter, Dialect dialect, NamedParameterJdbcOperations operations,
			QueryMappingConfiguration queryMapping) {

		SqlGeneratorSource source = new SqlGeneratorSource(mappingContext, converter, dialect);
		SqlParametersFactory spf = new SqlParametersFactory(mappingContext, converter);
		InsertStrategyFactory isf = new InsertStrategyFactory(operations, dialect);

		return new DataAccessStrategyFactory(source, converter, operations, spf, isf, queryMapping);
	}

}
