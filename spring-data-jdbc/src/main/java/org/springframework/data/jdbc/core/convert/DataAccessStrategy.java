/*
 * Copyright 2019 the original author or authors.
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

import java.util.Map;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.data.relational.domain.SqlIdentifier;
import org.springframework.lang.Nullable;

/**
 * Abstraction for accesses to the database that should be implementable with a single SQL statement per method and
 * relates to a single entity as opposed to {@link JdbcAggregateOperations} which provides interactions related to
 * complete aggregates.
 *
 * @author Jens Schauder
 * @author Tyler Van Gorder
 * @author Milan Milanov
 */
public interface DataAccessStrategy extends RelationResolver {

	/**
	 * Inserts a the data of a single entity. Referenced entities don't get handled.
	 *
	 * @param instance the instance to be stored. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @param additionalParameters name-value pairs of additional parameters. Especially ids of parent entities that need
	 *          to get referenced are contained in this map. Must not be {@code null}.
	 * @param <T> the type of the instance.
	 * @return the id generated by the database if any.
	 * @deprecated since 1.1, use {@link #insert(Object, Class, Identifier)} instead.
	 */
	@Deprecated
	@Nullable
	<T> Object insert(T instance, Class<T> domainType, Map<SqlIdentifier, Object> additionalParameters);

	/**
	 * Inserts a the data of a single entity. Referenced entities don't get handled.
	 *
	 * @param instance the instance to be stored. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @param identifier information about data that needs to be considered for the insert but which is not part of the
	 *          entity. Namely references back to a parent entity and key/index columns for entities that are stored in a
	 *          {@link Map} or {@link java.util.List}.
	 * @param <T> the type of the instance.
	 * @return the id generated by the database if any.
	 * @since 1.1
	 */
	@Nullable
	default <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {
		return insert(instance, domainType, identifier.toMap());
	}

	/**
	 * Updates the data of a single entity in the database. Referenced entities don't get handled.
	 *
	 * @param instance the instance to save. Must not be {@code null}.
	 * @param domainType the type of the instance to save. Must not be {@code null}.
	 * @param <T> the type of the instance to save.
	 * @return whether the update actually updated a row.
	 */
	<T> boolean update(T instance, Class<T> domainType);

	/**
	 * Updates the data of a single entity in the database and enforce optimistic record locking using the
	 * {@code previousVersion} property. Referenced entities don't get handled.
	 * <P>
	 * The statement will be of the form : {@code UPDATE … SET … WHERE ID = :id and VERSION_COLUMN = :previousVersion }
	 * and throw an optimistic record locking exception if no rows have been updated.
	 *
	 * @param instance the instance to save. Must not be {@code null}.
	 * @param domainType the type of the instance to save. Must not be {@code null}.
	 * @param previousVersion The previous version assigned to the instance being saved.
	 * @param <T> the type of the instance to save.
	 * @return whether the update actually updated a row.
	 * @throws OptimisticLockingFailureException if the update fails to update at least one row assuming the the
	 *           optimistic locking version check failed.
	 * @since 2.0
	 */
	<T> boolean updateWithVersion(T instance, Class<T> domainType, Number previousVersion);

	/**
	 * Deletes a single row identified by the id, from the table identified by the domainType. Does not handle cascading
	 * deletes.
	 * <P>
	 * The statement will be of the form : {@code DELETE FROM … WHERE ID = :id and VERSION_COLUMN = :version } and throw
	 * an optimistic record locking exception if no rows have been updated.
	 *
	 * @param id the id of the row to be deleted. Must not be {@code null}.
	 * @param domainType the type of entity to be deleted. Implicitly determines the table to operate on. Must not be
	 *          {@code null}.
	 */
	void delete(Object id, Class<?> domainType);

	/**
	 * Deletes a single entity from the database and enforce optimistic record locking using the version property. Does
	 * not handle cascading deletes.
	 *
	 * @param id the id of the row to be deleted. Must not be {@code null}.
	 * @param domainType the type of entity to be deleted. Implicitly determines the table to operate on. Must not be
	 *          {@code null}.
	 * @param previousVersion The previous version assigned to the instance being saved.
	 * @throws OptimisticLockingFailureException if the update fails to update at least one row assuming the the
	 *           optimistic locking version check failed.
	 * @since 2.0
	 */
	<T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion);

	/**
	 * Deletes all entities reachable via {@literal propertyPath} from the instance identified by {@literal rootId}.
	 *
	 * @param rootId Id of the root object on which the {@literal propertyPath} is based. Must not be {@code null}.
	 * @param propertyPath Leading from the root object to the entities to be deleted. Must not be {@code null}.
	 */
	void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath);

	/**
	 * Deletes all entities of the given domain type.
	 *
	 * @param domainType the domain type for which to delete all entries. Must not be {@code null}.
	 * @param <T> type of the domain type.
	 */
	<T> void deleteAll(Class<T> domainType);

	/**
	 * Deletes all entities reachable via {@literal propertyPath} from any instance.
	 *
	 * @param propertyPath Leading from the root object to the entities to be deleted. Must not be {@code null}.
	 */
	void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath);

	/**
	 * Counts the rows in the table representing the given domain type.
	 *
	 * @param domainType the domain type for which to count the elements. Must not be {@code null}.
	 * @return the count. Guaranteed to be not {@code null}.
	 */
	long count(Class<?> domainType);

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
	 * @param domainType the type of entities to laod. Must not be {@code null}.
	 * @param <T> type of entities to load.
	 * @return the loaded entities. Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.RelationResolver#findAllByPath(org.springframework.data.relational.domain.Identifier, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	default Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		Object rootId = identifier.toMap().get(path.getRequiredLeafProperty().getReverseColumnName());
		return findAllByProperty(rootId, path.getRequiredLeafProperty());
	};

	/**
	 * Finds all entities reachable via {@literal property} from the instance identified by {@literal rootId}.
	 *
	 * @param rootId Id of the root object on which the {@literal propertyPath} is based.
	 * @param property Leading from the root object to the entities to be found.
	 * @deprecated Use #findAllByPath instead.
	 */
	@Deprecated
	<T> Iterable<T> findAllByProperty(Object rootId, RelationalPersistentProperty property);

	/**
	 * returns if a row with the given id exists for the given type.
	 *
	 * @param id the id of the entity for which to check. Must not be {@code null}.
	 * @param domainType the type of the entity to check for. Must not be {@code null}.
	 * @param <T> the type of the entity.
	 * @return {@code true} if a matching row exists, otherwise {@code false}.
	 */
	<T> boolean existsById(Object id, Class<T> domainType);

	/**
	 * Loads all entities of the given type, sorted.
	 *
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> the type of entities to load.
	 * @param sort the sorting information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAll(Class<T> domainType, Sort sort);

	/**
	 * Loads all entities of the given type, paged and sorted.
	 *
	 * @param domainType the type of entities to load. Must not be {@code null}.
	 * @param <T> the type of entities to load.
	 * @param pageable the pagination information. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> Iterable<T> findAll(Class<T> domainType, Pageable pageable);
}
