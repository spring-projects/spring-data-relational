/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.r2dbc.repository.support;

import java.io.Serializable;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create
 * {@link org.springframework.data.r2dbc.repository.R2dbcRepository} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository
 */
public class R2dbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> {

	private @Nullable DatabaseClient client;
	private @Nullable MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;
	private @Nullable ReactiveDataAccessStrategy dataAccessStrategy;

	private boolean mappingContextConfigured = false;

	/**
	 * Creates a new {@link R2dbcRepositoryFactoryBean} for the given repository interface.
	 *
	 * @param repositoryInterface must not be {@literal null}.
	 */
	public R2dbcRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	/**
	 * Configures the {@link DatabaseClient} to be used.
	 *
	 * @param client the client to set
	 */
	public void setDatabaseClient(@Nullable DatabaseClient client) {
		this.client = client;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#setMappingContext(org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void setMappingContext(@Nullable MappingContext<?, ?> mappingContext) {

		super.setMappingContext(mappingContext);

		if (mappingContext != null) {

			this.mappingContext = (MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty>) mappingContext;
			this.mappingContextConfigured = true;
		}
	}

	public void setDataAccessStrategy(@Nullable ReactiveDataAccessStrategy dataAccessStrategy) {
		this.dataAccessStrategy = dataAccessStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#createRepositoryFactory()
	 */
	@Override
	protected final RepositoryFactorySupport createRepositoryFactory() {
		return getFactoryInstance(client, this.mappingContext);
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 *
	 * @param client must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @return new instance of {@link RepositoryFactorySupport}.
	 */
	protected RepositoryFactorySupport getFactoryInstance(DatabaseClient client,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return new R2dbcRepositoryFactory(client, mappingContext, dataAccessStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.state(client != null, "DatabaseClient must not be null!");
		Assert.state(dataAccessStrategy != null, "ReactiveDataAccessStrategy must not be null!");

		if (!mappingContextConfigured) {
			setMappingContext(new RelationalMappingContext());
		}

		super.afterPropertiesSet();
	}
}
