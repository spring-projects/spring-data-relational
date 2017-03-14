/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.mapping.event;

import org.springframework.data.jdbc.mapping.event.Identifier.Unset;

/**
 * Gets published before an entity gets inserted into the database. When the id-property of the entity must get set
 * manually, an event listener for this event may do so. <br>
 * The {@link Identifier} is {@link Unset#UNSET}
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class BeforeInsert extends BeforeSave {

	/**
	 * @param instance the entity about to get inserted.
	 */
	public BeforeInsert(Object instance) {
		super(Unset.UNSET, instance);
	}
}
