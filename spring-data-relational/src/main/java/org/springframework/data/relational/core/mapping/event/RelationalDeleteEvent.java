/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping.event;

import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Super class for events produced during deleting an aggregate. Such events have an {@link Identifier} and an
 * {@link AggregateChange} and may also have an entity if the entity was provided to the method performing the delete.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public abstract class RelationalDeleteEvent<E> extends AbstractRelationalEvent<E>
		implements WithId<E>, WithAggregateChange<E> {

	private static final long serialVersionUID = -8071323168471611098L;

	private final Identifier id;
	private final @Nullable E entity;
	private final AggregateChange<E> change;

	/**
	 * @param id the identifier of the aggregate that gets deleted. Must not be {@literal null}.
	 * @param entity is the aggregate root that gets deleted. Might be {@literal null}.
	 * @param change the {@link AggregateChange} for the deletion containing more detailed information about the deletion
	 */
	RelationalDeleteEvent(Identifier id, @Nullable E entity, AggregateChange<E> change) {

		super(id);

		Assert.notNull(id, "Id must not be null");
		Assert.notNull(change, "Change must not be null");

		this.id = id;
		this.entity = entity;
		this.change = change;
	}

	@Override
	public Identifier getId() {
		return id;
	}

	@Override
	@Nullable
	public E getEntity() {
		return entity;
	}

	@Override
	public AggregateChange<E> getAggregateChange() {
		return change;
	}

	@Override
	public Class<E> getType() {
		return change.getEntityType();
	}
}
