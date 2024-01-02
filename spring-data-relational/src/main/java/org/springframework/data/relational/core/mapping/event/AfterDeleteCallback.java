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

/**
 * An {@link EntityCallback} that gets called after an aggregate got deleted. This callback gets only invoked if the
 * method deleting the aggregate received an instance of that aggregate as an argument. Methods deleting entities by id
 * or without any parameter don't invoke this callback.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
@FunctionalInterface
public interface AfterDeleteCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked after an aggregate root was deleted. Can return either the same or a modified
	 * instance of the aggregate object.
	 *
	 * @param aggregate the aggregate that was deleted.
	 * @return the aggregate that was deleted.
	 */
	T onAfterDelete(T aggregate);
}
