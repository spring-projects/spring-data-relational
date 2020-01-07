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
 * Gets published after deletion of an entity. It will have a {@link Specified} identifier. If the entity is empty or
 * not depends on the delete method used.
 *
 * @author Jens Schauder
 */
public class AfterDeleteEvent extends RelationalEventWithId {

	private static final long serialVersionUID = 3594807189931141582L;

	/**
	 * @param id of the entity.
	 * @param instance the deleted entity if it is available.
	 * @param change the {@link AggregateChange} encoding the actions that were performed on the database as part of the
	 *          delete operation.
	 */
	public AfterDeleteEvent(Specified id, Optional<Object> instance, AggregateChange change) {
		super(id, instance, change);
	}
}
