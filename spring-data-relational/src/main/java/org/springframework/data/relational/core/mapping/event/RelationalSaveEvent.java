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
package org.springframework.data.relational.core.mapping.event;

import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.util.Assert;

/**
 * Events triggered during saving of an aggregate. Events of this type always have an {@link AggregateChange} and an
 * entity.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public abstract class RelationalSaveEvent<E> extends RelationalEventWithEntity<E> implements WithAggregateChange<E> {

	private final AggregateChange<E> change;

	RelationalSaveEvent(E entity, AggregateChange<E> change) {

		super(entity);

		Assert.notNull(change, "Change must not be null");
		this.change = change;
	}

	@Override
	public AggregateChange<E> getAggregateChange() {
		return change;
	}
}
