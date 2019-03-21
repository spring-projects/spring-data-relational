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
package org.springframework.data.relational.core.conversion;

import lombok.Value;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Represents a single entity in an aggregate along with its property path from the root entity and the chain of
 * objects to traverse a long this path.
 *
 * @author Jens Schauder
 */
@Value
class PathNode {

	/**
	 * The path to this entity
	 */
	PersistentPropertyPath<RelationalPersistentProperty> path;

	/**
	 * The parent {@link PathNode}. This is {@code null} if this is the root entity.
	 */
	@Nullable
	PathNode parent;

	/**
	 * The value of the entity.
	 */
	Object value;
}
