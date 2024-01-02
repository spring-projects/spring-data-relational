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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
class DefaultRootAggregateChange<T> implements RootAggregateChange<T> {

	private final Kind kind;

	/** Type of the aggregate root to be changed */
	private final Class<T> entityType;

	private final List<DbAction<?>> actions = new ArrayList<>();

	private DbAction.WithRoot<T> rootAction;

	/** The previous version assigned to the instance being changed, if available */
	@Nullable private final Number previousVersion;

	DefaultRootAggregateChange(Kind kind, Class<T> entityType, @Nullable Number previousVersion) {

		this.kind = kind;
		this.entityType = entityType;
		this.previousVersion = previousVersion;
	}

	/**
	 * Adds an action to this {@code AggregateChange}.
	 *
	 * @param action must not be {@literal null}.
	 */
	@Override
	public void addAction(DbAction<?> action) {

		Assert.notNull(action, "Action must not be null");

		actions.add(action);
	}

	@Override
	public Kind getKind() {
		return this.kind;
	}

	@Override
	public Class<T> getEntityType() {
		return this.entityType;
	}

	@Override
	public void setRoot(T aggregateRoot) {

		Assert.isInstanceOf(this.entityType, aggregateRoot,
				String.format("AggregateRoot must be of type %s", entityType.getName()));

		rootAction.setEntity(aggregateRoot);
	}

	@Override
	public void setRootAction(DbAction.WithRoot<T> action) {

		Assert.isNull(this.rootAction, "The rootAction must be set exactly once");

		this.rootAction = action;
	}

	@Nullable
	@Override
	public Number getPreviousVersion() {
		return previousVersion;
	}

	@Override
	public T getRoot() {
		return this.rootAction.getEntity();
	}

	@Override
	public void forEachAction(Consumer<? super DbAction<?>> consumer) {

		Assert.notNull(consumer, "Consumer must not be null");
		Assert.notNull(rootAction, "DbAction.WithRoot must not be null");

		consumer.accept(rootAction);
		actions.forEach(consumer);
	}
}
