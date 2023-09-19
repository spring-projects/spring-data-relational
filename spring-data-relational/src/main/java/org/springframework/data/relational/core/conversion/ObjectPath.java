/*
 * Copyright 2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A path of objects nested into each other. The type allows access to all parent objects currently in creation even
 * when resolving more nested objects. This allows to avoid re-resolving object instances that are logically equivalent
 * to already resolved ones.
 * <p>
 * An immutable ordered set of target objects for {@link org.springframework.data.relational.domain.RowDocument} to
 * {@link Object} conversions. Object paths can be extended via
 * {@link #push(Object, org.springframework.data.relational.core.mapping.RelationalPersistentEntity)}.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public final class ObjectPath {

	public static final ObjectPath ROOT = new ObjectPath();

	private final @Nullable ObjectPath parent;
	private final @Nullable Object object;

	private ObjectPath() {

		this.parent = null;
		this.object = null;
	}

	/**
	 * Creates a new {@link ObjectPath} from the given parent {@link ObjectPath} and adding the provided path values.
	 *
	 * @param parent must not be {@literal null}.
	 * @param object must not be {@literal null}.
	 */
	private ObjectPath(ObjectPath parent, Object object) {

		this.parent = parent;
		this.object = object;
	}

	/**
	 * Returns a copy of the {@link ObjectPath} with the given {@link Object} as current object.
	 *
	 * @param object must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return new instance of {@link ObjectPath}.
	 */
	ObjectPath push(Object object, RelationalPersistentEntity<?> entity) {

		Assert.notNull(object, "Object must not be null");
		Assert.notNull(entity, "RelationalPersistentEntity must not be null");

		return new ObjectPath(this, object);
	}

	/**
	 * Returns the current object of the {@link ObjectPath} or {@literal null} if the path is empty.
	 *
	 * @return
	 */
	@Nullable
	Object getCurrentObject() {
		return getObject();
	}

	@Nullable
	private Object getObject() {
		return object;
	}

	@Override
	public String toString() {

		if (parent == null) {
			return "[empty]";
		}

		List<String> strings = new ArrayList<>();

		for (ObjectPath current = this; current != null; current = current.parent) {
			strings.add(ObjectUtils.nullSafeToString(current.getObject()));
		}

		return StringUtils.collectionToDelimitedString(strings, " -> ");
	}
}
