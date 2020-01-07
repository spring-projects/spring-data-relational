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

import org.springframework.data.relational.core.conversion.AggregateChange;

/**
 * Gets published before an entity gets saved to the database. The contained {@link AggregateChange} is mutable and may
 * be changed in order to change the actions that get performed on the database as part of the save operation.
 *
 * @author Jens Schauder
 */
public class BeforeSaveEvent extends RelationalEventWithEntity {

	private static final long serialVersionUID = -6996874391990315443L;

	/**
	 * @param id of the entity to be saved.
	 * @param instance the entity about to get saved.
	 * @param change the {@link AggregateChange} that is going to get applied to the database.
	 */
	public BeforeSaveEvent(Identifier id, Object instance, AggregateChange change) {
		super(id, instance, change);
	}
}
