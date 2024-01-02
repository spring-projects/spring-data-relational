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

import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.query.Query;
import org.springframework.lang.Nullable;

/**
 * The finding methods of a {@link DataAccessStrategy}.
 *
 * @author Jens Schauder
 * @since 3.2
 */
interface ReadingDataAccessStrategy {

	/**
	 * Loads a single entity identified by type and id.
	 *
	 * @param id the id of the entity to load. Must not be {@code null}.
	 * @param domainType the domain type of the entity. Must not be {@code null}.
	 * @param <T> the type of the entity.
	 * @return Might return {@code null}.
	 */
	@Nullable
	<T> T findById(Object id, Class<T> domainType);

	/**
	 * Loads all entities of the given type.
	 *
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> the type of entities to load.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAll(Class<T> domainType);

	/**
	 * Loads all entities that match one of the ids passed as an argument. It is not guaranteed that the number of ids
	 * passed in matches the number of entities returned.
	 *
	 * @param ids the Ids of the entities to load. Must not be {@code null}.
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> type of entities to load.
	 * @return the loaded entities. Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType);

	/**
	 * Loads all entities of the given type, sorted.
	 *
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> the type of entities to load.
	 * @param sort the sorting information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 * @since 2.0
	 */
	<T> Iterable<T> findAll(Class<T> domainType, Sort sort);

	/**
	 * Loads all entities of the given type, paged and sorted.
	 *
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> the type of entities to load.
	 * @param pageable the pagination information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 * @since 2.0
	 */
	<T> Iterable<T> findAll(Class<T> domainType, Pageable pageable);

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity ensuring exactly one result.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the type of entities. Must not be {@code null}.
	 * @return exactly one result or {@link Optional#empty()} if no match found.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 * @since 3.0
	 */
	<T> Optional<T> findOne(Query query, Class<T> domainType);

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link Iterable}.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the type of entities. Must not be {@code null}.
	 * @return a non-null list with all the matching results.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 * @since 3.0
	 */
	<T> Iterable<T> findAll(Query query, Class<T> domainType);

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link Iterable}. Applies the {@link Pageable}
	 * to the result.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the type of entities. Must not be {@literal  null}.
	 * @param pageable the pagination that should be applied. Must not be {@literal null}.
	 * @return a non-null list with all the matching results.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 * @since 3.0
	 */
	<T> Iterable<T> findAll(Query query, Class<T> domainType, Pageable pageable);
}
