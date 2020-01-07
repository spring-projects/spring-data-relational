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
package org.springframework.data.relational.core.mapping.event;

import java.util.Optional;

import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.mapping.event.Identifier.Specified;

/**
 * Gets published when an entity is about to get deleted. The contained {@link AggregateChange} is mutable and may be
 * changed in order to change the actions that get performed on the database as part of the delete operation.
 *
 * @author Jens Schauder
 */
public class BeforeDeleteEvent extends RelationalEventWithId {

	private static final long serialVersionUID = -5483432053368496651L;

	/**
	 * @param id the id of the entity
	 * @param entity the entity about to get deleted. Might be empty.
	 * @param change the {@link AggregateChange} encoding the planned actions to be performed on the database.
	 */
	public <T> BeforeDeleteEvent(Specified id, Optional<Object> entity, AggregateChange change) {
		super(id, entity, change);
	}
}
