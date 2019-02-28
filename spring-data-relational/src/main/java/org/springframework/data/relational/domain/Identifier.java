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
package org.springframework.data.relational.domain;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * {@literal Identifier} represents a multi part id of an entity. Parts or all of the entity might not have a
 * representation as a property in the entity but might only be derived from other entities referencing it.
 * 
 * @author Jens Schauder
 * @since 1.1
 */
public final class Identifier {

	private final List<SingleIdentifierValue> keys;

	private Identifier(List<SingleIdentifierValue> keys) {
		this.keys = keys;
	}

	static public Identifier empty() {
		return new Identifier(Collections.emptyList());
	}

	static public Identifier simple(String name, Object value, Class<?> targetType) {
		return new Identifier(Collections.singletonList(new SingleIdentifierValue(name, value, targetType)));
	}

	public Identifier add(String name, Object value, Class<?> targetType) {

		List<SingleIdentifierValue> keys = new ArrayList<>(this.keys);
		keys.add(new SingleIdentifierValue(name, value, targetType));
		return new Identifier(keys);
	}

	@Deprecated
	public Map<String, Object> getParametersByName() {

		HashMap<String, Object> result = new HashMap<>();
		forEach(v -> result.put(v.name, v.value));
		return result;
	}

	public Collection<SingleIdentifierValue> getParameters() {
		return keys;
	}

	public void forEach(Consumer<SingleIdentifierValue> consumer) {
		getParameters().forEach(consumer);
	}

	/**
	 * A single value of an Identifier consisting of the column name, the value and the target type which is to be used to
	 * store the element in the database.
	 *
	 * @author Jens Schauder
	 * @since 1.1
	 */
	@Value
	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	public static class SingleIdentifierValue {

		String name;
		Object value;
		Class<?> targetType;
	}
}
