/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.mapping.event;

import org.reactivestreams.Publisher;

import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Entity callback triggered before save of a row.
 *
 * @author Mark Paluch
 * @since 1.2
 * @see org.springframework.data.mapping.callback.ReactiveEntityCallbacks
 */
@FunctionalInterface
public interface BeforeSaveCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before a domain object is saved. Can return either the same or a modified instance
	 * of the domain object and can modify {@link OutboundRow} contents. This method is called after converting the
	 * {@code entity} to a {@link OutboundRow} so effectively the row is used as outcome of invoking this callback.
	 * Changes to the domain object are not taken into account for saving, only changes to the row. Only transient fields
	 * of the entity should be changed in this callback. To change persistent the entity before being converted, use the
	 * {@link BeforeConvertCallback}.
	 *
	 * @param entity the domain object to save.
	 * @param row {@link OutboundRow} representing the {@code entity}.
	 * @param table name of the table.
	 * @return the domain object to be persisted.
	 */
	Publisher<T> onBeforeSave(T entity, OutboundRow row, SqlIdentifier table);
}
