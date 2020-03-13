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
 * Gets published after a new instance or a changed instance was saved in the database.
 *
 * @author Jens Schauder
 */
public class AfterSaveEvent extends RelationalSaveEvent {

	private static final long serialVersionUID = 1681164473866370396L;

	/**
	 * @param instance the saved entity. Must not be {@literal null}.
	 * @param change the {@link AggregateChange} encoding the actions performed on the database as part of the delete.
	 *          Must not be {@literal null}.
	 */
	public AfterSaveEvent(Object instance, AggregateChange<?> change) {
		super(instance, change);
	}
}
