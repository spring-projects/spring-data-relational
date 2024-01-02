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

/**
 * Gets published after deletion of an entity. It will have a {@link Identifier} identifier. If the entity is
 * {@literal null} or not depends on the delete method used.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class AfterDeleteEvent<E> extends RelationalDeleteEvent<E> {

	private static final long serialVersionUID = 2615043444207870206L;

	/**
	 * @param id of the entity. Must not be {@literal null}.
	 * @param instance the deleted entity if it is available. May be {@literal null}.
	 * @param change the {@link AggregateChange} encoding the actions that were performed on the database as part of the
	 *          delete operation. Must not be {@literal null}.
	 */
	public AfterDeleteEvent(Identifier id, @Nullable E instance, AggregateChange<E> change) {
		super(id, instance, change);
	}
}
