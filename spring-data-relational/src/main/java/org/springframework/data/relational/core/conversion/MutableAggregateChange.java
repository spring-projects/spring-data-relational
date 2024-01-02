/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Chirag Tailor
 * @since 2.0
 */
public interface MutableAggregateChange<T> extends AggregateChange<T> {

	/**
	 * Factory method to create a {@link RootAggregateChange} for saving entities.
	 *
	 * @param entity aggregate root to save.
	 * @param <T> entity type.
	 * @return the {@link RootAggregateChange} for saving the root {@code entity}.
	 * @since 1.2
	 */
	static <T> RootAggregateChange<T> forSave(T entity) {
		return forSave(entity, null);
	}

	/**
	 * Factory method to create a {@link RootAggregateChange} for saving entities.
	 *
	 * @param entity aggregate root to save.
	 * @param previousVersion the previous version assigned to the instance being saved. May be {@literal null}.
	 * @param <T> entity type.
	 * @return the {@link RootAggregateChange} for saving the root {@code entity}.
	 * @since 2.4
	 */
	@SuppressWarnings("unchecked")
	static <T> RootAggregateChange<T> forSave(T entity, @Nullable Number previousVersion) {

		Assert.notNull(entity, "Entity must not be null");

		return new DefaultRootAggregateChange<>(Kind.SAVE, (Class<T>) ClassUtils.getUserClass(entity), previousVersion);
	}

	/**
	 * Factory method to create a {@link DeleteAggregateChange} for deleting entities.
	 *
	 * @param entity aggregate root to delete.
	 * @param <T> entity type.
	 * @return the {@link DeleteAggregateChange} for deleting the root {@code entity}.
	 * @since 1.2
	 */
	@SuppressWarnings("unchecked")
	static <T> DeleteAggregateChange<T> forDelete(T entity) {

		Assert.notNull(entity, "Entity must not be null");

		return forDelete((Class<T>) ClassUtils.getUserClass(entity));
	}

	/**
	 * Factory method to create a {@link DeleteAggregateChange} for deleting entities.
	 *
	 * @param entityClass aggregate root type.
	 * @param <T> entity type.
	 * @return the {@link DeleteAggregateChange} for deleting the root {@code entity}.
	 * @since 1.2
	 */
	static <T> DeleteAggregateChange<T> forDelete(Class<T> entityClass) {
		return forDelete(entityClass, null);
	}

	/**
	 * Factory method to create a {@link DeleteAggregateChange} for deleting entities.
	 *
	 * @param entityClass aggregate root type.
	 * @param previousVersion the previous version assigned to the instance being saved. May be {@literal null}.
	 * @param <T> entity type.
	 * @return the {@link DeleteAggregateChange} for deleting the root {@code entity}.
	 * @since 2.4
	 */
	static <T> DeleteAggregateChange<T> forDelete(Class<T> entityClass, @Nullable Number previousVersion) {

		Assert.notNull(entityClass, "Entity class must not be null");

		return new DeleteAggregateChange<>(entityClass, previousVersion);
	}

	/**
	 * Adds an action to this {@code AggregateChange}.
	 *
	 * @param action must not be {@literal null}.
	 */
	void addAction(DbAction<?> action);

	@Nullable
	Number getPreviousVersion();
}
