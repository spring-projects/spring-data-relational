/*
 * Copyright 2017 the original author or authors.
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
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.core.support.TransactionalRepositoryFactoryBeanSupport;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Special adapter for Springs {@link org.springframework.beans.factory.FactoryBean} interface to allow easy setup of
 * repository factories via Spring configuration.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class JdbcRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> //
		extends TransactionalRepositoryFactoryBeanSupport<T, S, ID> {

	private static final String NO_NAMED_PARAMETER_JDBC_OPERATION_ERROR_MESSAGE = //
			"No unique NamedParameterJdbcOperation could be found, " //
					+ "nor JdbcOperations or DataSource to construct one from.";

	private final ApplicationEventPublisher applicationEventPublisher;
	private final ApplicationContext context;

	JdbcRepositoryFactoryBean(Class<? extends T> repositoryInterface, ApplicationEventPublisher applicationEventPublisher,
			ApplicationContext context) {

		super(repositoryInterface);
		this.applicationEventPublisher = applicationEventPublisher;
		this.context = context;
	}

	@Override
	protected RepositoryFactorySupport doCreateRepositoryFactory() {
		return new JdbcRepositoryFactory(findOrCreateJdbcOperations(), applicationEventPublisher);
	}

	private NamedParameterJdbcOperations findOrCreateJdbcOperations() {

		return getNamedParameterJdbcOperations() //
				.orElseGet(() -> getJdbcOperations().map(NamedParameterJdbcTemplate::new) //
						.orElseGet(() -> getDataSource().map(NamedParameterJdbcTemplate::new) //
								.orElseThrow(() -> new IllegalStateException( //
										NO_NAMED_PARAMETER_JDBC_OPERATION_ERROR_MESSAGE //
		))));
	}

	private Optional<NamedParameterJdbcOperations> getNamedParameterJdbcOperations() {
		return getBean(NamedParameterJdbcOperations.class, "namedParameterJdbcOperations");
	}

	private Optional<JdbcOperations> getJdbcOperations() {
		return getBean(JdbcOperations.class, "jdbcOperations");
	}

	private Optional<DataSource> getDataSource() {
		return getBean(DataSource.class, "dataSource");
	}

	private <R> Optional<R> getBean(Class<R> type, String name) {

		Map<String, R> beansOfType = context.getBeansOfType(type);

		if (beansOfType.size() == 1) {
			return beansOfType.values().stream().findFirst();
		}

		return Optional.ofNullable(beansOfType.get(name));
	}
}
