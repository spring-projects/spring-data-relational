/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class MutableAggregateChange<T> implements AggregateChange<T> {

	private final Kind kind;

	/** Type of the aggregate root to be changed */
	private final Class<T> entityType;

	private final List<DbAction<?>> actions = new ArrayList<>();
	/** Aggregate root, to which the change applies, if available */
	@Nullable private T entity;

	public MutableAggregateChange(Kind kind, Class<T> entityType, @Nullable T entity) {

		this.kind = kind;
		this.entityType = entityType;
		this.entity = entity;
	}

	/**
	 * Factory method to create an {@link MutableAggregateChange} for saving entities.
	 *
	 * @param entity aggregate root to save.
	 * @param <T> entity type.
	 * @return the {@link MutableAggregateChange} for saving the root {@code entity}.
	 * @since 1.2
	 */
	@SuppressWarnings("unchecked")
	public static <T> MutableAggregateChange<T> forSave(T entity) {

		Assert.notNull(entity, "Entity must not be null");
		return new MutableAggregateChange<>(Kind.SAVE, (Class<T>) ClassUtils.getUserClass(entity), entity);
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
	public static <T> MutableAggregateChange<T> forDelete(T entity) {

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
	public static <T> MutableAggregateChange<T> forDelete(Class<T> entityClass, @Nullable T entity) {

		Assert.notNull(entityClass, "Entity class must not be null");
		return new MutableAggregateChange<>(Kind.DELETE, entityClass, entity);
	}

	/**
	 * Adds an action to this {@code AggregateChange}.
	 * 
	 * @param action must not be {@literal null}.
	 */
	public void addAction(DbAction<?> action) {

		Assert.notNull(action, "Action must not be null.");

		actions.add(action);
	}

	/**
	 * Applies the given consumer to each {@link DbAction} in this {@code AggregateChange}.
	 * 
	 * @param consumer must not be {@literal null}.
	 */
	@Override
	public void forEachAction(Consumer<? super DbAction<?>> consumer) {

		Assert.notNull(consumer, "Consumer must not be null.");

		actions.forEach(consumer);
	}

	/**
	 * Returns the {@link Kind} of {@code AggregateChange} this is.
	 * 
	 * @return guaranteed to be not {@literal null}.
	 */
	@Override
	public Kind getKind() {
		return this.kind;
	}

	/**
	 * The type of the root of this {@code AggregateChange}.
	 * 
	 * @return Guaranteed to be not {@literal null}.
	 */
	@Override
	public Class<T> getEntityType() {
		return this.entityType;
	}

	/**
	 * Set the root object of the {@code AggregateChange}.
	 *
	 * @param aggregateRoot may be {@literal null} if the change refers to a list of aggregates or references it by id.
	 */
	public void setEntity(@Nullable T aggregateRoot) {

		if (aggregateRoot != null) {
			Assert.isInstanceOf(entityType, aggregateRoot,
					String.format("AggregateRoot must be of type %s", entityType.getName()));
		}

		entity = aggregateRoot;
	}

	/**
	 * The entity to which this {@link MutableAggregateChange} relates.
	 * 
	 * @return may be {@literal null}.
	 */
	@Override
	@Nullable
	public T getEntity() {
		return this.entity;
	}

}
