/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.relational.domain;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@literal Identifier} represents a composite id of an entity that may be composed of one or many parts. Parts or all
 * of the entity might not have a representation as a property in the entity but might only be derived from other
 * entities referencing it.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
@EqualsAndHashCode
@ToString
public final class Identifier {

	private static final Identifier EMPTY = new Identifier(Collections.emptyList());

	private final List<SingleIdentifierValue> parts;

	private Identifier(List<SingleIdentifierValue> parts) {
		this.parts = parts;
	}

	/**
	 * Returns an empty {@link Identifier}.
	 *
	 * @return an empty {@link Identifier}.
	 */
	public static Identifier empty() {
		return EMPTY;
	}

	/**
	 * Creates an {@link Identifier} from {@code name}, {@code value}, and a {@link Class target type}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param value
	 * @param targetType must not be {@literal null}.
	 * @return the {@link Identifier} for {@code name}, {@code value}, and a {@link Class target type}.
	 */
	public static Identifier of(SqlIdentifier name, Object value, Class<?> targetType) {

		Assert.notNull(name, "Name must not be empty!");
		Assert.notNull(targetType, "Target type must not be null!");

		return new Identifier(Collections.singletonList(new SingleIdentifierValue(name, value, targetType)));
	}

	/**
	 * Creates an {@link Identifier} from a {@link Map} of name to value tuples.
	 *
	 * @param map must not be {@literal null}.
	 * @return the {@link Identifier} from a {@link Map} of name to value tuples.
	 */
	public static Identifier from(Map<SqlIdentifier, Object> map) {

		Assert.notNull(map, "Map must not be null!");

		if (map.isEmpty()) {
			return empty();
		}

		List<SingleIdentifierValue> values = new ArrayList<>();

		map.forEach((k, v) -> {

			values.add(new SingleIdentifierValue(k, v, v != null ? ClassUtils.getUserClass(v) : Object.class));
		});

		return new Identifier(Collections.unmodifiableList(values));
	}

	/**
	 * Creates a new {@link Identifier} from the current instance and sets the value for {@code key}. Existing key
	 * definitions for {@code name} are overwritten if they already exist.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param value
	 * @param targetType must not be {@literal null}.
	 * @return the {@link Identifier} containing all existing keys and the key part for {@code name}, {@code value}, and a
	 *         {@link Class target type}.
	 */
	public Identifier withPart(SqlIdentifier name, Object value, Class<?> targetType) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(targetType, "Target type must not be null!");

		boolean overwritten = false;
		List<SingleIdentifierValue> keys = new ArrayList<>(this.parts.size() + 1);

		for (SingleIdentifierValue singleValue : this.parts) {

			if (singleValue.getName().equals(name)) {
				overwritten = true;
				keys.add(new SingleIdentifierValue(singleValue.getName(), value, targetType));
			} else {
				keys.add(singleValue);
			}
		}

		if (!overwritten) {
			keys.add(new SingleIdentifierValue(name, value, targetType));
		}

		return new Identifier(Collections.unmodifiableList(keys));
	}

	/**
	 * Returns a {@link Map} containing the identifier name to value tuples.
	 *
	 * @return a {@link Map} containing the identifier name to value tuples.
	 */
	public Map<SqlIdentifier, Object> toMap() {

		Map<SqlIdentifier, Object> result = new LinkedHashMap<>();
		forEach((name, value, type) -> result.put(name, value));
		return result;
	}

	/**
	 * @return the {@link SingleIdentifierValue key parts}.
	 */
	public Collection<SingleIdentifierValue> getParts() {
		return this.parts;
	}

	/**
	 * Performs the given action for each element of the {@link Identifier} until all elements have been processed or the
	 * action throws an exception. Unless otherwise specified by the implementing class, actions are performed in the
	 * order of iteration (if an iteration order is specified). Exceptions thrown by the action are relayed to the caller.
	 *
	 * @param consumer the action, must not be {@literal null}.
	 */
	public void forEach(IdentifierConsumer consumer) {

		Assert.notNull(consumer, "IdentifierConsumer must not be null");

		getParts().forEach(it -> consumer.accept(it.name, it.value, it.targetType));
	}

	/**
	 * Returns the number of key parts in this collection.
	 *
	 * @return the number of key parts in this collection.
	 */
	public int size() {
		return this.parts.size();
	}

	/**
	 * A single value of an Identifier consisting of the column name, the value and the target type which is to be used to
	 * store the element in the database.
	 *
	 * @author Jens Schauder
	 */
	@Value
	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	static class SingleIdentifierValue {

		SqlIdentifier name;
		Object value;
		Class<?> targetType;
	}

	/**
	 * Represents an operation that accepts identifier key parts (name, value and {@link Class target type}) defining a
	 * contract to consume {@link Identifier} values.
	 *
	 * @author Mark Paluch
	 */
	@FunctionalInterface
	public interface IdentifierConsumer {

		/**
		 * Performs this operation on the given arguments.
		 * 
		 * @param name
		 * @param value
		 * @param targetType
		 */
		void accept(SqlIdentifier name, Object value, Class<?> targetType);
	}
}
