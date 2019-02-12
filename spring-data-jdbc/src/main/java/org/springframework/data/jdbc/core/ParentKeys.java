/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * @author Jens Schauder
 */
public class ParentKeys {

	private final List<ParentKey> keys;

	/**
	 * Only provided as a bridge to the old way of passing additional parameters to an insert statement.
	 *
	 * @param additionalParameters
	 */
	@Deprecated
	static ParentKeys fromNamedValues(Map<String, Object> additionalParameters) {

		List<ParentKey> keys = new ArrayList<>();
		additionalParameters.forEach((k, v) -> keys.add(new ParentKey(k, v, v == null ? Object.class : v.getClass())));

		ParentKeys parentKeys = new ParentKeys(keys);
		return parentKeys;
	}

	/**
	 * Creates ParentKeys with backreference for the given path and value of the parents id.
	 */
	static ParentKeys forBackReferences(PersistentPropertyPath<RelationalPersistentProperty> path, @Nullable Object value) {

		ParentKey parentKey = new ParentKey( //
				path.getRequiredLeafProperty().getReverseColumnName(), //
				value, //
				getLastIdProperty(path).getColumnType() //
		);

		return new ParentKeys(Collections.singletonList(parentKey));
	}

	private static RelationalPersistentProperty getLastIdProperty(
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		RelationalPersistentProperty idProperty = path.getRequiredLeafProperty().getOwner().getIdProperty();

		if (idProperty != null) {
			return idProperty;
		}

		return getLastIdProperty(path.getParentPath());
	}

	private ParentKeys(List<ParentKey> keys) {

		this.keys = Collections.unmodifiableList(keys);
	}

	ParentKeys withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value) {

		List<ParentKey> keys = new ArrayList<>(this.keys);

		RelationalPersistentProperty leafProperty = path.getRequiredLeafProperty();
		keys.add(new ParentKey(leafProperty.getKeyColumn(), value, leafProperty.getQualifierColumnType()));

		return new ParentKeys(keys);
	}

	@Deprecated
	public Map<String, Object> getParametersByName() {
		return new HashMap<>();
	}

	Collection<ParentKey> getParameters() {
		return keys;
	}

	@Value
	static class ParentKey {
		String name;
		Object value;
		Class<?> targetType;
	}
}
