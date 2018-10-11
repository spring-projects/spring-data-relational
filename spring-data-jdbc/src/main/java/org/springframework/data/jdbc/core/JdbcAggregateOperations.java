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
package org.springframework.data.jdbc.core;

import org.springframework.lang.Nullable;

/**
 * Specifies a operations one can perform on a database, based on an <em>Domain Type</em>.
 *
 * @author Jens Schauder
 */
public interface JdbcAggregateOperations {

	/**
	 * Saves an instance of an aggregate, including all the members of the aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be saved. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 * @return the saved instance.
	 */
	<T> T save(T instance);

	/**
	 * Deletes a single Aggregate including all entities contained in that aggregate.
	 *
	 * @param id the id of the aggregate root of the aggregate to be deleted. Must not be {@code null}.
	 * @param domainType the type of the aggregate root.
	 * @param <T> the type of the aggregate root.
	 */
	<T> void deleteById(Object id, Class<T> domainType);

	/**
	 * Delete an aggregate identified by it's aggregate root.
	 *
	 * @param aggregateRoot to delete. Must not be {@code null}.
	 * @param domainType the type of the aggregate root. Must not be {@code null}.
	 * @param <T> the type of the aggregate root.
	 */
	<T> void delete(T aggregateRoot, Class<T> domainType);

	/**
	 * Delete all aggregates of a given type.
	 *
	 * @param domainType type of the aggregate roots to be deleted. Must not be {@code null}.
	 */
	void deleteAll(Class<?> domainType);

	/**
	 * Counts the number of aggregates of a given type.
	 *
	 * @param domainType the type of the aggregates to be counted.
	 * @return the number of instances stored in the database. Guaranteed to be not {@code null}.
	 */
	long count(Class<?> domainType);

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
	<T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType);

	/**
	 * Load all aggregates of a given type.
	 *
	 * @param domainType the type of the aggregate roots. Must not be {@code null}.
	 * @param <T> the type of the aggregate roots. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAll(Class<T> domainType);

	/**
	 * Checks if an aggregate identified by type and id exists in the database.
	 *
	 * @param id the id of the aggregate root.
	 * @param domainType the type of the aggregate root.
	 * @param <T> the type of the aggregate root.
	 * @return whether the aggregate exists.
	 */
	<T> boolean existsById(Object id, Class<T> domainType);
}
