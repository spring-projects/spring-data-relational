/*
 * Copyright 2017-2023 the original author or authors.
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

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Chirag Tailor
 * @since 2.0
 */
class DefaultAggregateChange<T> implements MutableAggregateChange<T> {

	private final Kind kind;

	/** Type of the aggregate root to be changed */
	private final Class<T> entityType;

	private final List<DbAction<?>> actions = new ArrayList<>();

	/** Aggregate root, to which the change applies, if available */
	private @Nullable T entity;

	/** The previous version assigned to the instance being changed, if available */
	@Nullable private final Number previousVersion;

	public DefaultAggregateChange(Kind kind, Class<T> entityType, @Nullable T entity, @Nullable Number previousVersion) {

		this.kind = kind;
		this.entityType = entityType;
		this.entity = entity;
		this.previousVersion = previousVersion;
	}

	/**
	 * Adds an action to this {@code AggregateChange}.
	 *
	 * @param action must not be {@literal null}.
	 */
	@Override
	public void addAction(DbAction<?> action) {

		Assert.notNull(action, "Action must not be null.");

		actions.add(action);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.AggregateChange#getKind()
	 */
	@Override
	public Kind getKind() {
		return this.kind;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.AggregateChange#getEntityType()
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
	@Override
	public void setEntity(@Nullable T aggregateRoot) {

		if (aggregateRoot != null) {
			Assert.isInstanceOf(this.entityType, aggregateRoot,
					String.format("AggregateRoot must be of type %s", entityType.getName()));
		}

		this.entity = aggregateRoot;
	}

	@Nullable
	@Override
	public Number getPreviousVersion() {
		return previousVersion;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.AggregateChange#getEntity()
	 */
	@Override
	public T getEntity() {
		return this.entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.AggregateChange#forEachAction(java.util.function.Consumer)
	 */
	@Override
	public void forEachAction(Consumer<? super DbAction<?>> consumer) {

		Assert.notNull(consumer, "Consumer must not be null.");

		actions.forEach(consumer);
	}
}
