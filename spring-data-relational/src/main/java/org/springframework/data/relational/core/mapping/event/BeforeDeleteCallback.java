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
 * An {@link EntityCallback} that gets invoked before an entity is deleted. This callback gets only invoked if the
 * method deleting the aggregate received an instance of that aggregate as an argument. Methods deleting entities by id
 * or without any parameter don't invoke this callback.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
@FunctionalInterface
public interface BeforeDeleteCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before an aggregate root is deleted. Can return either the same or a modified
	 * instance of the aggregate and can modify {@link MutableAggregateChange} contents. This method is called after
	 * converting the {@code aggregate} to {@link MutableAggregateChange}. Changes to the aggregate are not taken into
	 * account for deleting. Only transient fields of the entity should be changed in this callback.
	 *
	 * @param aggregate the aggregate.
	 * @param aggregateChange the associated {@link MutableAggregateChange}.
	 * @return the aggregate to be deleted.
	 */
	T onBeforeDelete(T aggregate, MutableAggregateChange<T> aggregateChange);
}
