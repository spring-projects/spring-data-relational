/*
 * Copyright 2020 the original author or authors.
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
 * @since 2.0
 */
public interface MutableAggregateChange<T> extends AggregateChange<T> {

	/**
	 * Factory method to create an {@link MutableAggregateChange} for saving entities.
	 *
	 * @param entity aggregate root to save.
	 * @param <T> entity type.
	 * @return the {@link MutableAggregateChange} for saving the root {@code entity}.
	 * @since 1.2
	 */
	@SuppressWarnings("unchecked")
	static <T> MutableAggregateChange<T> forSave(T entity) {

		Assert.notNull(entity, "Entity must not be null");

		return new DefaultAggregateChange<>(Kind.SAVE, (Class<T>) ClassUtils.getUserClass(entity), entity);
	}

	/**
	 * Factory method to create an {@link MutableAggregateChange} for deleting entities.
	 *
	 * @param entity aggregate root to delete.
	 * @param <T> entity type.
	 * @return the {@link MutableAggregateChange} for deleting the root {@code entity}.
	 * @since 1.2
	 */
	@SuppressWarnings("unchecked")
	static <T> MutableAggregateChange<T> forDelete(T entity) {

		Assert.notNull(entity, "Entity must not be null");

		return forDelete((Class<T>) ClassUtils.getUserClass(entity), entity);
	}

	/**
	 * Factory method to create an {@link MutableAggregateChange} for deleting entities.
	 *
	 * @param entityClass aggregate root type.
	 * @param entity aggregate root to delete.
	 * @param <T> entity type.
	 * @return the {@link MutableAggregateChange} for deleting the root {@code entity}.
	 * @since 1.2
	 */
	static <T> MutableAggregateChange<T> forDelete(Class<T> entityClass, @Nullable T entity) {

		Assert.notNull(entityClass, "Entity class must not be null");

		return new DefaultAggregateChange<>(Kind.DELETE, entityClass, entity);
	}

	/**
	 * Adds an action to this {@code AggregateChange}.
	 *
	 * @param action must not be {@literal null}.
	 */
	void addAction(DbAction<?> action);

	/**
	 * Set the root object of the {@code AggregateChange}.
	 *
	 * @param aggregateRoot may be {@literal null} if the change refers to a list of aggregates or references it by id.
	 */
	void setEntity(@Nullable T aggregateRoot);
}
