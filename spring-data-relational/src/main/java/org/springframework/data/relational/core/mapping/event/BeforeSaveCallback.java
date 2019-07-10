/*
 * Copyright 2019 the original author or authors.
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
 * An {@link EntityCallback} that gets invoked before changes get applied to the database but after the aggregate got actually converted to a database change.
 *
 * @since 1.1
 * @author Jens Schauder
 */
@FunctionalInterface
public interface BeforeSaveCallback<T> extends EntityCallback<T> {

	T onBeforeSave(T aggregate, Identifier id);
}
