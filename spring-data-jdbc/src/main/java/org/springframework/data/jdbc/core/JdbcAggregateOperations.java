/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.util.List;
import java.util.Optional;

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.query.Query;
import org.springframework.lang.Nullable;

/**
 * Specifies operations one can perform on a database, based on an <em>Domain Type</em>.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Milan Milanov
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @author Myeonghyeon Lee
 */
public interface JdbcAggregateOperations {

	/**
	 * Saves an instance of an aggregate, including all the members of the aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be saved. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instance.
	 * @throws IncorrectUpdateSemanticsDataAccessException when the instance is determined to be not new and the resulting
	 *           update does not update any rows.
	 */
	<T> T save(T instance);

	/**
	 * Saves all aggregate instances, including all the members of each aggregate instance.
	 *
	 * @param instances the aggregate roots to be saved. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instances.
	 * @throws IncorrectUpdateSemanticsDataAccessException when at least one instance is determined to be not new and the
	 *           resulting update does not update any rows.
	 * @since 3.0
	 */
	<T> List<T> saveAll(Iterable<T> instances);

	/**
	 * Dedicated insert function. This skips the test if the aggregate root is new and makes an insert.
	 * <p>
	 * This is useful if the client provides an id for new aggregate roots.
	 * </p>
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instance.
	 */
	<T> T insert(T instance);

	/**
	 * Inserts all aggregate instances, including all the members of each aggregate instance.
	 * <p>
	 * This is useful if the client provides an id for new aggregate roots.
	 * </p>
	 *
	 * @param instances the aggregate roots to be inserted. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instances.
	 * @since 3.1
	 */
	<T> Iterable<T> insertAll(Iterable<T> instances);

	/**
	 * Dedicated update function. This skips the test if the aggregate root is new or not and always performs an update
	 * operation.
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instance.
	 */
	<T> T update(T instance);

	/**
	 * Updates all aggregate instances, including all the members of each aggregate instance.
	 *
	 * @param instances the aggregate roots to be inserted. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instances.
	 * @since 3.1
	 */
	<T> List<T> updateAll(Iterable<T> instances);

	/**
	 * Counts the number of aggregates of a given type.
	 *
	 * @param domainType the type of the aggregates to be counted.
	 * @return the number of instances stored in the database. Guaranteed to be not {@code null}.
	 */
	long count(Class<?> domainType);

	/**
	 * Counts the number of aggregates of a given type that match the given <code>query</code>.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the entity type must not be {@literal null}.
	 * @return the number of instances stored in the database. Guaranteed to be not {@code null}.
	 * @since 3.0
	 */
	<T> long count(Query query, Class<T> domainType);

	/**
	 * Determine whether there are aggregates that match the {@link Query}
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the entity type must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @since 3.0
	 */
	<T> boolean exists(Query query, Class<T> domainType);

	/**
	 * Checks if an aggregate identified by type and id exists in the database.
	 *
	 * @param id the id of the aggregate root.
	 * @param domainType the type of the aggregate root.
	 * @param <T> the type of the aggregate root.
	 * @return whether the aggregate exists.
	 */
	<T> boolean existsById(Object id, Class<T> domainType);

	/**
	 * Load an aggregate from the database.
	 *
	 * @param id the id of the aggregate to load. Must not be {@code null}.
	 * @param domainType the type of the aggregate root. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the loaded aggregate. Might return {@code null}.
	 */
	@Nullable
	<T> T findById(Object id, Class<T> domainType);

	/**
	 * Load all aggregates of a given type that are identified by the given ids.
	 *
	 * @param ids of the aggregate roots identifying the aggregates to load. Must not be {@code null}.
	 * @param domainType the type of the aggregate roots. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> List<T> findAllById(Iterable<?> ids, Class<T> domainType);

	/**
	 * Load all aggregates of a given type.
	 *
	 * @param domainType the type of the aggregate roots. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> List<T> findAll(Class<T> domainType);

	/**
	 * Load all aggregates of a given type, sorted.
	 *
	 * @param domainType the type of the aggregate roots. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots. Must not be {@code null}.
	 * @param sort the sorting information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 * @since 2.0
	 */
	<T> List<T> findAll(Class<T> domainType, Sort sort);

	/**
	 * Load a page of (potentially sorted) aggregates of a given type.
	 *
	 * @param domainType the type of the aggregate roots. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots. Must not be {@code null}.
	 * @param pageable the pagination information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 * @since 2.0
	 */
	<T> Page<T> findAll(Class<T> domainType, Pageable pageable);

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity ensuring exactly one result.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the entity type must not be {@literal null}.
	 * @return exactly one result or {@link Optional#empty()} if no match found.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 * @since 3.0
	 */
	<T> Optional<T> findOne(Query query, Class<T> domainType);

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a {@link List} that is sorted.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the entity type must not be {@literal null}.
	 * @return a non-null sorted list with all the matching results.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 * @since 3.0
	 */
	<T> List<T> findAll(Query query, Class<T> domainType);

	/**
	 * Returns a {@link Page} of entities matching the given {@link Query}. In case no match could be found, an empty
	 * {@link Page} is returned.
	 *
	 * @param query must not be {@literal null}.
	 * @param domainType the entity type must not be {@literal null}.
	 * @param pageable can be null.
	 * @return a {@link Page} of entities matching the given {@link Example}.
	 * @since 3.0
	 */
	<T> Page<T> findAll(Query query, Class<T> domainType, Pageable pageable);

	/**
	 * Deletes a single Aggregate including all entities contained in that aggregate.
	 * <p>
	 * Since no version attribute is provided this method will never throw a
	 * {@link org.springframework.dao.OptimisticLockingFailureException}. If no rows match the generated delete operation
	 * this fact will be silently ignored.
	 * </p>
	 *
	 * @param id the id of the aggregate root of the aggregate to be deleted. Must not be {@code null}.
	 * @param domainType the type of the aggregate root.
	 * @param <T> the type of the aggregate root.
	 */
	<T> void deleteById(Object id, Class<T> domainType);

	/**
	 * Deletes all aggregates identified by their aggregate root ids.
	 * <p>
	 * Since no version attribute is provided this method will never throw a
	 * {@link org.springframework.dao.OptimisticLockingFailureException}. If no rows match the generated delete operation
	 * this fact will be silently ignored.
	 * </p>
	 *
	 * @param ids the ids of the aggregate roots of the aggregates to be deleted. Must not be {@code null}.
	 * @param domainType the type of the aggregate root.
	 * @param <T> the type of the aggregate root.
	 */
	<T> void deleteAllById(Iterable<?> ids, Class<T> domainType);

	/**
	 * Delete an aggregate identified by its aggregate root.
	 *
	 * @param aggregateRoot to delete. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 */
	<T> void delete(T aggregateRoot);

	/**
	 * Delete all aggregates of a given type.
	 *
	 * @param domainType type of the aggregate roots to be deleted. Must not be {@code null}.
	 */
	void deleteAll(Class<?> domainType);

	/**
	 * Delete all aggregates identified by their aggregate roots.
	 *
	 * @param aggregateRoots to delete. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots.
	 */
	<T> void deleteAll(Iterable<? extends T> aggregateRoots);
}
