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
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
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
	private RowMapperMap rowMapperMap = RowMapperMap.EMPTY;

	public JdbcRepositoryFactory(ApplicationEventPublisher publisher, JdbcMappingContext context,
			DataAccessStrategy dataAccessStrategy) {

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> aClass) {
		return (EntityInformation<T, ID>) context.getRequiredPersistentEntityInformation(aClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object getTargetRepository(RepositoryInformation repositoryInformation) {

		JdbcPersistentEntityInformation persistentEntityInformation = context
				.getRequiredPersistentEntityInformation(repositoryInformation.getDomainType());
		JdbcAggregateTemplate template = new JdbcAggregateTemplate(publisher, context, accessStrategy);

		return new SimpleJdbcRepository<>(template, persistentEntityInformation);
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata repositoryMetadata) {
		return SimpleJdbcRepository.class;
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(QueryLookupStrategy.Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {

		if (key != null //
				&& key != QueryLookupStrategy.Key.USE_DECLARED_QUERY //
				&& key != QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND //
		) {
			throw new IllegalArgumentException(String.format("Unsupported query lookup strategy %s!", key));
		}

		return Optional.of(new JdbcQueryLookupStrategy(evaluationContextProvider, context, accessStrategy, rowMapperMap));
	}

	/**
	 * @param rowMapperMap must not be {@literal null} consider {@link RowMapperMap#EMPTY} instead.
	 */
	public void setRowMapperMap(RowMapperMap rowMapperMap) {

		Assert.notNull(rowMapperMap, "RowMapperMap must not be null!");
		this.rowMapperMap = rowMapperMap;
	}
}
