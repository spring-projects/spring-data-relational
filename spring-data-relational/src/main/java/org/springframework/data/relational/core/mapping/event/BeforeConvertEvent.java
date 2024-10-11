/*
 * Copyright 2017-2024 the original author or authors.
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

import java.io.Serial;

/**
 * Gets published before an aggregate gets converted into a database change, but after the decision was made if an
 * insert or an update is to be performed.
 * <p>
 * The persisting process works as follows:
 * <ol>
 * <li>A decision is made, if the aggregate is new and therefore should be inserted or if it is not new and therefore
 * should be updated.</li>
 * <li>{@link BeforeConvertCallback} and {@link BeforeConvertEvent} get published.</li>
 * <li>An {@link org.springframework.data.relational.core.conversion.AggregateChange} object is created for the
 * aggregate. It includes the {@link org.springframework.data.relational.core.conversion.DbAction} instances to be
 * executed. This means that all the deletes, updates and inserts to be performed are determined. These actions
 * reference entities of the aggregates in order to access values to be used in the SQL statements. This step also
 * determines if the id of an entity gets passed to the database or if the database is expected to generate that id.</li>
 * <li>{@link BeforeSaveCallback} and {@link BeforeSaveEvent} get published.</li>
 * <li>SQL statements get applied to the database.</li>
 * <li>{@link AfterSaveCallback} and {@link AfterSaveEvent} get published.</li>
 * </ol>
 *
 * @since 1.1
 * @author Jens Schauder
 * @author Mark Paluch
 * @see BeforeConvertCallback
 */
public class BeforeConvertEvent<E> extends RelationalEventWithEntity<E> {

	@Serial
	private static final long serialVersionUID = -5716795164911939224L;

	/**
	 * @param instance the saved entity. Must not be {@literal null}.
	 * @since 2.1.4
	 */
	public BeforeConvertEvent(E instance) {
		super(instance);
	}
}
