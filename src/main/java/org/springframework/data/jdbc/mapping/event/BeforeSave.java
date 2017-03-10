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

import java.util.Optional;

/**
 * Subclasses of this get published before an entity gets saved to the database.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class BeforeSave extends JdbcEventWithEntity {

	/**
	 * @param id of the entity to be saved.
	 * @param instance the entity about to get saved.
	 */
	BeforeSave(Identifier id, Object instance) {
		super(id, instance);
	}
}
