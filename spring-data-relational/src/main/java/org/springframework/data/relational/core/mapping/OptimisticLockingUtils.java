/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Utility methods to create {@link OptimisticLockingFailureException}s.
 * <p>
 * Strictly for internal use within the framework.
 *
 * @author Mark Paluch
 * @since 3.5.7
 */
public class OptimisticLockingUtils {

	/**
	 * Create an {@link OptimisticLockingFailureException} for an update failure.
	 *
	 * @param entity the object.
	 * @param version the object version.
	 * @param persistentEntity the {@link RelationalPersistentEntity} metadata.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException updateFailed(Object entity, @Nullable Object version,
			RelationalPersistentEntity<?> persistentEntity) {

		IdentifierAccessor identifierAccessor = persistentEntity.getIdentifierAccessor(entity);
		Object id = identifierAccessor.getRequiredIdentifier();

		return new OptimisticLockingFailureException(String.format(
				"Failed to update versioned entity with id '%s' (version '%s') in table [%s]; Was the entity updated or deleted concurrently?",
				id, version, persistentEntity.getTableName()));
	}

	/**
	 * Create an {@link OptimisticLockingFailureException} for a delete failure.
	 *
	 * @param entity actual entity to be deleted.
	 * @param persistentEntity the {@link RelationalPersistentEntity} metadata.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException deleteFailed(Object entity,
			RelationalPersistentEntity<?> persistentEntity) {

		IdentifierAccessor identifierAccessor = persistentEntity.getIdentifierAccessor(entity);
		Object id = identifierAccessor.getRequiredIdentifier();
		PersistentProperty<?> versionProperty = persistentEntity.getRequiredVersionProperty();
		Object version = persistentEntity.getPropertyAccessor(entity).getProperty(versionProperty);

		return deleteFailed(id, version, persistentEntity);
	}

	/**
	 * Create an {@link OptimisticLockingFailureException} for a delete failure.
	 *
	 * @param id the object identifier.
	 * @param version the object version.
	 * @param persistentEntity the {@link RelationalPersistentEntity} metadata.
	 * @return the exception.
	 */
	public static OptimisticLockingFailureException deleteFailed(@Nullable Object id, @Nullable Object version,
			RelationalPersistentEntity<?> persistentEntity) {

		return new OptimisticLockingFailureException(String.format(
				"Failed to delete versioned entity with id '%s' (version '%s') in table [%s]; Was the entity updated or deleted concurrently?",
				id, version, persistentEntity.getTableName()));
	}

}
