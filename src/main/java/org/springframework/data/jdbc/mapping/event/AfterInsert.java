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

import org.springframework.data.jdbc.mapping.event.Identifier.Specified;

/**
 * Gets published after an entity got inserted into the database.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class AfterInsert extends AfterSave {

	/**
	 * @param id identifier of the entity triggering the event.
	 * @param instance the newly inserted entity.
	 */
	public AfterInsert(Specified id, Object instance) {
		super(id, instance);
	}
}
