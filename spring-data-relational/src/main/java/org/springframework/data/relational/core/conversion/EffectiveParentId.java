/*
 * Copyright 2018 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Represents the Id of a table. Note that this differs from the Id of an entity since it also includes a backreference
 * to the owning entity if the the entity is not an aggregate root.
 *
 * @author Jens Schauder
 */
public class EffectiveParentId {

	private final Map<String, Object> internal = new HashMap<>();
	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> keys = new HashMap<>();

	public Map<String, Object> toParameterMap(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		RelationalPersistentProperty property = (path == null) ? null : path.getRequiredLeafProperty();

		HashMap<String, Object> parameterMap = new HashMap<>(internal);
		keys.forEach((keySourcePath, value) -> {

			if (keySourcePath != null) {
				RelationalPersistentProperty leafProperty = keySourcePath.getLeafProperty();

				if (leafProperty != null && leafProperty.isQualified()) {

					parameterMap.put(property.getKeyColumn(keySourcePath), value);
				}
			} else {
				parameterMap.put(property.getReverseColumnName(keySourcePath, path), value);
			}
		});
		return parameterMap;
	}

	public void putAll(Map<String, Object> additionalValues) {
		internal.putAll(additionalValues);
	}

	public void put(String columnName, Object identifier) {
		internal.put(columnName, identifier);
	}

	public void addKey(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path, Object value) {
		keys.put(path, value);
	};
}
