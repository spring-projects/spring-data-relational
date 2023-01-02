/*
 * Copyright 2019-2023 the original author or authors.
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
 * An {@link EntityCallback} that gets invoked before the aggregate is converted into a database change. The decision if
 * the change will be an insert or update is already made when this callback gets called.
 * <p>
 * The main use of this kind of callback is to create application side Ids for aggregates where this isn't done on the
 * database side.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
@FunctionalInterface
public interface BeforeConvertCallback<T> extends EntityCallback<T> {

	/**
	 * Entity callback method invoked before an aggregate root is converted to be persisted. Can return either the same or
	 * a modified instance of the aggregate.
	 *
	 * @param aggregate the saved aggregate.
	 * @return the aggregate to be persisted.
	 */
	T onBeforeConvert(T aggregate);
}
