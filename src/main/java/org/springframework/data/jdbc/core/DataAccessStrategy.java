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

import java.util.Map;

import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyPath;

/**
 * Abstraction for accesses to the database that should be implementable with a single SQL statement and relates to a
 * single entity as opposed to {@link JdbcEntityOperations} which provides interactions related to complete aggregates.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public interface DataAccessStrategy {

	<T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters);

	<T> void update(T instance, Class<T> domainType);

	void delete(Object id, Class<?> domainType);

	/**
	 * Deletes all entities reachable via {@literal propertyPath} from the instance identified by {@literal rootId}.
	 *
	 * @param rootId Id of the root object on which the {@literal propertyPath} is based.
	 * @param propertyPath Leading from the root object to the entities to be deleted.
	 */
	void delete(Object rootId, PropertyPath propertyPath);

	<T> void deleteAll(Class<T> domainType);

	/**
	 * Deletes all entities reachable via {@literal propertyPath} from any instance.
	 *
	 * @param propertyPath Leading from the root object to the entities to be deleted.
	 */
	<T> void deleteAll(PropertyPath propertyPath);

	long count(Class<?> domainType);

	<T> T findById(Object id, Class<T> domainType);

	<T> Iterable<T> findAll(Class<T> domainType);

	<T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType);

	/**
	 * Finds all entities reachable via {@literal property} from the instance identified by {@literal rootId}.
	 *
	 * @param rootId Id of the root object on which the {@literal propertyPath} is based.
	 * @param property Leading from the root object to the entities to be found.
	 */
	<T> Iterable<T> findAllByProperty(Object rootId, JdbcPersistentProperty property);

	<T> boolean existsById(Object id, Class<T> domainType);
}
