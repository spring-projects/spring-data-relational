/*
 * Copyright 2017-2019 the original author or authors.
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
import org.springframework.data.relational.core.mapping.event.Identifier.Specified;

/**
 * Gets published before an aggregate gets converted into a database change.
 *
 * @since 1.1
 * @author Jens Schauder
 */
public class BeforeConvertEvent extends RelationalEventWithIdAndEntity {

	private static final long serialVersionUID = 3980149746683849019L;

	/**
	 * @param id identifier of the saved entity.
	 * @param instance the saved entity.
	 * @param change the {@link AggregateChange} encoding the actions performed on the database as part of the delete.
	 */
	public BeforeConvertEvent(Specified id, Object instance, AggregateChange change) {
		super(id, instance, change);
	}
}
