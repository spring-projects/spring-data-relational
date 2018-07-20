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

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

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
	private RelationalMappingContext mappingContext;
	private RelationalConverter converter;
	private DataAccessStrategy dataAccessStrategy;
	private RowMapperMap rowMapperMap = RowMapperMap.EMPTY;
	private NamedParameterJdbcOperations operations;

	/**
	 * Creates a new {@link JdbcRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	JdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
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
		jdbcRepositoryFactory.setRowMapperMap(rowMapperMap);

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
	@Autowired(required = false)
	public void setDataAccessStrategy(DataAccessStrategy dataAccessStrategy) {
		this.dataAccessStrategy = dataAccessStrategy;
	}

	/**
	 * @param rowMapperMap can be {@literal null}. {@link #afterPropertiesSet()} defaults to {@link RowMapperMap#EMPTY} if
	 *          {@literal null}.
	 */
	@Autowired(required = false)
	public void setRowMapperMap(RowMapperMap rowMapperMap) {
		this.rowMapperMap = rowMapperMap;
	}

	@Autowired
	public void setJdbcOperations(NamedParameterJdbcOperations operations) {
		this.operations = operations;
	}

	@Autowired
	public void setConverter(RelationalConverter converter) {
		this.converter = converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.state(this.mappingContext != null, "MappingContext is required and must not be null!");
		Assert.state(this.converter != null, "RelationalConverter is required and must not be null!");

		if (dataAccessStrategy == null) {

			SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(mappingContext);
			this.dataAccessStrategy = new DefaultDataAccessStrategy(sqlGeneratorSource, mappingContext, converter,
					operations);
		}

		if (rowMapperMap == null) {
			this.rowMapperMap = RowMapperMap.EMPTY;
		}

		super.afterPropertiesSet();
	}
}
