/*
 * Copyright 2022-2024 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * Represents the changes happening to one or more aggregates (as used in the context of Domain Driven Design) as a
 * whole. This change allows additional {@link MutableAggregateChange} of a particular kind to be added to it to
 * broadly represent the changes to multiple aggregates across all such added changes.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
public interface BatchingAggregateChange<T, C extends MutableAggregateChange<T>> extends AggregateChange<T> {
	/**
	 * Adds a {@code MutableAggregateChange} into this {@code BatchingAggregateChange}.
	 *
	 * @param aggregateChange must not be {@literal null}.
	 */
	void add(C aggregateChange);

	/**
	 * Factory method to create a {@link BatchingAggregateChange} for saving entities.
	 *
	 * @param entityClass aggregate root type.
	 * @param <T> entity type.
	 * @return the {@link BatchingAggregateChange} for saving root entities.
	 * @since 3.0
	 */
	static <T> BatchingAggregateChange<T, RootAggregateChange<T>> forSave(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null");

		return new SaveBatchingAggregateChange<>(entityClass);
	}

	/**
	 * Factory method to create a {@link BatchingAggregateChange} for deleting entities.
	 *
	 * @param entityClass aggregate root type.
	 * @param <T> entity type.
	 * @return the {@link BatchingAggregateChange} for deleting root entities.
	 * @since 3.0
	 */
	static <T> BatchingAggregateChange<T, DeleteAggregateChange<T>> forDelete(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null");

		return new DeleteBatchingAggregateChange<>(entityClass);
	}
}
