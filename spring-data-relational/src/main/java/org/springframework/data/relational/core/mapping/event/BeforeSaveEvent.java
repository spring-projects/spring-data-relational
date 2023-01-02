/*
 * Copyright 2017-2023 the original author or authors.
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

import org.springframework.context.ApplicationEvent;
import org.springframework.data.relational.core.conversion.AggregateChange;

/**
 * An {@link ApplicationEvent} that gets triggered before changes are applied to the database, after the aggregate was
 * converted to a database change.
 * <p>
 * Changes to the aggregate are not taken into account to decide whether the change will be an insert or update. Use the
 * {@link BeforeConvertCallback} to change the persistent the entity before being converted.
 * <p>
 * Currently a {@link BeforeSaveEvent} may be used to create an Id for new aggregate roots. This is for backward
 * compatibility purposes within the 2.x development line. The preferred way to do this is to use a
 * {@link BeforeConvertCallback} or {@link BeforeConvertEvent} and beginning with 3.0. this will be the only correct way
 * to do this.
 *
 * @author Jens Schauder
 */
public class BeforeSaveEvent<E> extends RelationalSaveEvent<E> {

	private static final long serialVersionUID = -4935804431519314116L;

	/**
	 * @param instance the entity about to get saved. Must not be {@literal null}.
	 * @param change the {@link AggregateChange} that is going to get applied to the database. Must not be
	 *          {@literal null}.
	 */
	public BeforeSaveEvent(E instance, AggregateChange<E> change) {
		super(instance, change);
	}
}
