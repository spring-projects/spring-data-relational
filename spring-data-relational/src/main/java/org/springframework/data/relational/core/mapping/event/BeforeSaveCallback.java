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
package org.springframework.data.relational.core.mapping.event;

import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;

/**
 * An {@link EntityCallback} that gets invoked before changes are applied to the database, after the aggregate was
 * converted to a database change.
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
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
@FunctionalInterface
public interface BeforeSaveCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before an aggregate root is saved. Can return either the same or a modified instance
	 * of the aggregate and can modify {@link MutableAggregateChange} contents. This method is called after converting the
	 * {@code aggregate} to {@link MutableAggregateChange}. Changes to the aggregate are not taken into account to decide
	 * whether the change will be an insert or update. Use the {@link BeforeConvertCallback} to change the persistent the
	 * entity before being converted.
	 *
	 * @param aggregate the aggregate.
	 * @param aggregateChange the associated {@link MutableAggregateChange}.
	 * @return the aggregate object to be persisted.
	 */
	T onBeforeSave(T aggregate, MutableAggregateChange<T> aggregateChange);
}
