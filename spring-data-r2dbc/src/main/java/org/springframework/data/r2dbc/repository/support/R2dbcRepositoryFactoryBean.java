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
package org.springframework.data.r2dbc.repository.support;

import java.io.Serializable;
import java.util.Optional;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create
 * {@link org.springframework.data.r2dbc.repository.R2dbcRepository} instances. Can be either configured with
 * {@link R2dbcEntityOperations} or {@link DatabaseClient} with {@link ReactiveDataAccessStrategy}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see org.springframework.data.repository.reactive.ReactiveSortingRepository
 */
public class R2dbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
		extends RepositoryFactoryBeanSupport<T, S, ID> implements ApplicationContextAware {

	private @Nullable DatabaseClient client;
	private @Nullable ReactiveDataAccessStrategy dataAccessStrategy;
	private @Nullable R2dbcEntityOperations operations;
	private @Nullable ApplicationContext applicationContext;

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
	public void setDatabaseClient(DatabaseClient client) {
		this.client = client;
	}

	public void setDataAccessStrategy(ReactiveDataAccessStrategy dataAccessStrategy) {
		this.dataAccessStrategy = dataAccessStrategy;
	}

	/**
	 * @param operations
	 * @since 1.1.3
	 */
	public void setEntityOperations(R2dbcEntityOperations operations) {
		this.operations = operations;
	}

	@Override
	protected void setMappingContext(MappingContext<?, ?> mappingContext) {

		this.mappingContextConfigured = true;
		super.setMappingContext(mappingContext);
	}

	@Override
	protected final RepositoryFactorySupport createRepositoryFactory() {

		return this.operations != null ? getFactoryInstance(this.operations)
				: getFactoryInstance(this.client, this.dataAccessStrategy);
	}

	@Override
	protected Optional<QueryMethodEvaluationContextProvider> createDefaultQueryMethodEvaluationContextProvider(
			ListableBeanFactory beanFactory) {
		return Optional.of(new ReactiveExtensionAwareQueryMethodEvaluationContextProvider(beanFactory));
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 *
	 * @param client must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @return new instance of {@link RepositoryFactorySupport}.
	 */
	protected RepositoryFactorySupport getFactoryInstance(DatabaseClient client,
			ReactiveDataAccessStrategy dataAccessStrategy) {
		return new R2dbcRepositoryFactory(client, dataAccessStrategy);
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 *
	 * @param operations must not be {@literal null}.
	 * @return new instance of {@link RepositoryFactorySupport}.
	 * @since 1.1.3
	 */
	protected RepositoryFactorySupport getFactoryInstance(R2dbcEntityOperations operations) {
		return new R2dbcRepositoryFactory(operations);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterPropertiesSet() {

		if (operations == null) {

			Assert.state(client != null, "DatabaseClient must not be null when R2dbcEntityOperations is not configured");
			Assert.state(dataAccessStrategy != null,
					"ReactiveDataAccessStrategy must not be null when R2dbcEntityOperations is not configured");

			R2dbcEntityTemplate template = new R2dbcEntityTemplate(client, dataAccessStrategy);

			if (applicationContext != null) {
				template.setApplicationContext(applicationContext);
			}

			operations = template;
		} else {
			dataAccessStrategy = operations.getDataAccessStrategy();
		}

		if (!mappingContextConfigured) {
			setMappingContext(dataAccessStrategy.getConverter().getMappingContext());
		}

		super.afterPropertiesSet();
	}
}
