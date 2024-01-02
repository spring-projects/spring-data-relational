/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.jdbc.core.convert;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.Query;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * A {@link ReadingDataAccessStrategy} that uses an {@link AggregateReader} to load entities with a single query.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 3.2
 */
class SingleQueryDataAccessStrategy implements ReadingDataAccessStrategy {

	private final RelationalMappingContext mappingContext;
	private final AggregateReader aggregateReader;

	public SingleQueryDataAccessStrategy(Dialect dialect, JdbcConverter converter,
			NamedParameterJdbcOperations jdbcTemplate) {

		this.mappingContext = converter.getMappingContext();
		this.aggregateReader = new AggregateReader(dialect, converter, jdbcTemplate);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return aggregateReader.findById(id, getPersistentEntity(domainType));
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType) {
		return aggregateReader.findAll(getPersistentEntity(domainType));
	}

	@Override
	public <T> List<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return aggregateReader.findAllById(ids, getPersistentEntity(domainType));
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType, Sort sort) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType, Pageable pageable) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> domainType) {
		return Optional.ofNullable(aggregateReader.findOne(query, getPersistentEntity(domainType)));
	}

	@Override
	public <T> List<T> findAll(Query query, Class<T> domainType) {
		return aggregateReader.findAll(query, getPersistentEntity(domainType));
	}

	@Override
	public <T> List<T> findAll(Query query, Class<T> domainType, Pageable pageable) {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	private <T> RelationalPersistentEntity<T> getPersistentEntity(Class<T> domainType) {
		return (RelationalPersistentEntity<T>) mappingContext.getRequiredPersistentEntity(domainType);
	}
}
