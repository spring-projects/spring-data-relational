/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping.event;

import org.springframework.data.jdbc.core.conversion.AggregateChange;
import org.springframework.data.jdbc.core.mapping.event.Identifier.Specified;

/**
 * Subclasses of this get published after a new instance or a changed instance was saved in the database.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class AfterSaveEvent extends JdbcEventWithIdAndEntity {

	private static final long serialVersionUID = 8982085767296982848L;

	/**
	 * @param id identifier of
	 * @param instance the newly saved entity.
	 * @param change the {@link AggregateChange} encoding the planned actions to be performed on the database.
	 */
	public AfterSaveEvent(Specified id, Object instance, AggregateChange change) {
		super(id, instance, change);
	}
}
