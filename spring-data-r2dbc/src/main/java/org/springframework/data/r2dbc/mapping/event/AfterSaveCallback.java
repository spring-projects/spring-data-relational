/*
 * Copyright 2020-2024 the original author or authors.
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
 * Entity callback triggered after save of a {@link OutboundRow}.
 *
 * @author Mark Paluch
 * @since 1.2
 * @see org.springframework.data.mapping.callback.ReactiveEntityCallbacks
 */
@FunctionalInterface
public interface AfterSaveCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked after a domain object is saved. Can return either the same or a modified instance of
	 * the domain object.
	 *
	 * @param entity the domain object that was saved.
	 * @param outboundRow {@link OutboundRow} representing the {@code entity}.
	 * @param table name of the table.
	 * @return the domain object that was persisted.
	 */
	Publisher<T> onAfterSave(T entity, OutboundRow outboundRow, SqlIdentifier table);
}
