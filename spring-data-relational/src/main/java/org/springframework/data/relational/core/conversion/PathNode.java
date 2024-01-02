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
package org.springframework.data.relational.core.conversion;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;

/**
 * Represents a single entity in an aggregate along with its property path from the root entity and the chain of objects
 * to traverse a long this path.
 *
 * @author Jens Schauder
 */
final class PathNode {

	/**
	 * The path to this entity
	 */
	private final PersistentPropertyPath<RelationalPersistentProperty> path;

	/**
	 * The parent {@link PathNode}. This is {@code null} if this is the root entity.
	 */
	@Nullable private final PathNode parent;

	/**
	 * The value of the entity.
	 */
	private final Object value;

	PathNode(PersistentPropertyPath<RelationalPersistentProperty> path, @Nullable PathNode parent, Object value) {

		this.path = path;
		this.parent = parent;
		this.value = value;
	}

	/**
	 * If the node represents a qualified property (i.e. a {@link java.util.List} or {@link java.util.Map}) the actual
	 * value is an element of the {@literal List} or a value of the {@literal Map}, while the {@link #value} is actually a
	 * {@link Pair} with the index or key as the first element and the actual value as second element.
	 */
	Object getActualValue() {

		return getPath().getLeafProperty().isQualified() //
				? ((Pair<?,?>) getValue()).getSecond() //
				: getValue();
	}

	public PersistentPropertyPath<RelationalPersistentProperty> getPath() {
		return this.path;
	}

	@Nullable
	public PathNode getParent() {
		return this.parent;
	}

	public Object getValue() {
		return this.value;
	}

	@Override
	public String toString() {

		return "PathNode{" + "path=" + path + ", parent=" + parent + ", value=" + value + '}';
	}
}
