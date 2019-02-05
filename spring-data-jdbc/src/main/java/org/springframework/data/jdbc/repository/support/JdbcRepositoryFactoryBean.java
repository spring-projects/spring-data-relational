/*
 * Copyright 2017-2019 the original author or authors.
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

import java.io.Serializable;
import java.util.Map;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Special adapter for Springs {@link org.springframework.beans.factory.FactoryBean} interface to allow easy setup of
 * repository factories via Spring configuration.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class JdbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> //
		extends TransactionalRepositoryFactoryBeanSupport<T, S, ID> implements ApplicationEventPublisherAware {

	private ApplicationEventPublisher publisher;
	private ConfigurableListableBeanFactory beanFactory;
	private RelationalMappingContext mappingContext;
	private RelationalConverter converter;
	private DataAccessStrategy dataAccessStrategy;
	private QueryMappingConfiguration queryMappingConfiguration = QueryMappingConfiguration.EMPTY;
	private NamedParameterJdbcOperations operations;
	private String dataAccessStrategyRef;
	private String jdbcOperationsRef;

	/**
	 * Creates a new {@link JdbcRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	protected JdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
	 */
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

		JdbcRepositoryFactory jdbcRepositoryFactory = new JdbcRepositoryFactory(dataAccessStrategy, mappingContext,
				converter, publisher, operations);
		jdbcRepositoryFactory.setQueryMappingConfiguration(queryMappingConfiguration);

		return jdbcRepositoryFactory;
	}

	@Autowired
	protected void setMappingContext(RelationalMappingContext mappingContext) {

		super.setMappingContext(mappingContext);
		this.mappingContext = mappingContext;
	}

	/**
	 * @param dataAccessStrategy can be {@literal null}.
	 */
	public void setDataAccessStrategy(DataAccessStrategy dataAccessStrategy) {
		this.dataAccessStrategy = dataAccessStrategy;
	}

	public void setDataAccessStrategyRef(String dataAccessStrategyRef) {
		this.dataAccessStrategyRef = dataAccessStrategyRef;
	}

	/**
	 * @param queryMappingConfiguration can be {@literal null}. {@link #afterPropertiesSet()} defaults to
	 *          {@link QueryMappingConfiguration#EMPTY} if {@literal null}.
	 */
	@Autowired(required = false)
	public void setQueryMappingConfiguration(QueryMappingConfiguration queryMappingConfiguration) {
		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	/**
	 * @param rowMapperMap can be {@literal null}. {@link #afterPropertiesSet()} defaults to {@link RowMapperMap#EMPTY} if
	 *          {@literal null}.
	 *
	 * @deprecated use {@link #setQueryMappingConfiguration(QueryMappingConfiguration)} instead.
	 */
	@Deprecated
	@Autowired(required = false)
	public void setRowMapperMap(RowMapperMap rowMapperMap) {
		setQueryMappingConfiguration(rowMapperMap);
	}

	public void setJdbcOperations(NamedParameterJdbcOperations operations) {
		this.operations = operations;
	}

	public void setJdbcOperationsRef(String jdbcOperationsRef) {
		this.jdbcOperationsRef = jdbcOperationsRef;
	}

	@Autowired
	public void setConverter(RelationalConverter converter) {
		this.converter = converter;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {

		super.setBeanFactory(beanFactory);

		this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.state(this.mappingContext != null, "MappingContext is required and must not be null!");
		Assert.state(this.converter != null, "RelationalConverter is required and must not be null!");

		ensureJdbcOperationsIsInitialized();
		ensureDataAccessStrategyIsInitialized();

		if (queryMappingConfiguration == null) {
			this.queryMappingConfiguration = QueryMappingConfiguration.EMPTY;
		}

		super.afterPropertiesSet();
	}

	private void ensureJdbcOperationsIsInitialized() {

		if (operations != null) {
			return;
		}

		operations = findBeanByNameOrType(NamedParameterJdbcOperations.class, jdbcOperationsRef);

	}

	private void ensureDataAccessStrategyIsInitialized() {

		if (dataAccessStrategy != null) {
			return;
		}

		dataAccessStrategy = findBeanByNameOrType(DataAccessStrategy.class, dataAccessStrategyRef);

		if (dataAccessStrategy != null) {
			return;
		}

		SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(mappingContext);
		this.dataAccessStrategy = new DefaultDataAccessStrategy(sqlGeneratorSource, mappingContext, converter,
				operations);
	}

	@Nullable
	private <B> B findBeanByNameOrType(Class<B> beanType, String beanName) {

		if (beanFactory == null) {
			return null;
		}

		if (!StringUtils.isEmpty(beanName)) {
			return beanFactory.getBean(beanName, beanType);
		}

		Map<String, B> beansOfType = beanFactory.getBeansOfType(beanType);

		if (beansOfType.size() == 0) {
			return null;
		}

		if (beansOfType.size() == 1) {
			return beansOfType.values().iterator().next();
		}

		for (Map.Entry<String, B> entry : beansOfType.entrySet()) {
			if (beanFactory.getBeanDefinition(entry.getKey()).isPrimary()) {
				return entry.getValue();
			}
		}

		throw new NoUniqueBeanDefinitionException(beanType, beansOfType.keySet());
	}
}
