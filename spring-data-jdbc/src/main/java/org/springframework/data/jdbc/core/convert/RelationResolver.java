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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Resolves relations within an aggregate.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public interface RelationResolver {

	/**
	 * Finds all entities reachable via {@literal path}.
	 *
	 * @param identifier the combination of Id, map keys and list indexes that identify the parent of the entity to be
	 *          loaded. Must not be {@literal null}.
	 * @param path the path from the aggregate root to the entities to be resolved. Must not be {@literal null}.
	 * @return guaranteed to be not {@literal null}.
	 */
	Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path);
}
