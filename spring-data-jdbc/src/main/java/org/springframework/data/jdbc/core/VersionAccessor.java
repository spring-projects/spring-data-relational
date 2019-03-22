/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * Helper class for convenient access to the value of an entity's version field, which can have one of several numeric
 * types.
 *
 * @param <T> type of the persistent entity
 * @author Tom Hombergs
 */
class VersionAccessor<T> {

	private T instance;
	private final RelationalPersistentProperty versionProperty;

	VersionAccessor(T instance, RelationalPersistentEntity<T> entity) {
		this.instance = instance;
		this.versionProperty = entity.getVersionProperty();
		Assert.notNull(versionProperty, "Entity must have a @Version property.");
	}

	Number nextVersion() {
		PersistentPropertyAccessor<T> accessor = versionProperty.getOwner().getPropertyAccessor(instance);
		Object currentValue = accessor.getProperty(versionProperty);
		return VersionMapper.forType(versionProperty.getType()).next(currentValue);
	}

	Number currentVersion() {
		PersistentPropertyAccessor<T> accessor = versionProperty.getOwner().getPropertyAccessor(instance);
		Object currentValue = accessor.getProperty(versionProperty);
		return VersionMapper.forType(versionProperty.getType()).get(currentValue);
	}

	void setVersion(Number newVersion) {
		PersistentPropertyAccessor<T> accessor = versionProperty.getOwner().getPropertyAccessor(instance);
		accessor.setProperty(versionProperty, newVersion);
	}

	/**
	 * Maps an object to a numeric value.
	 */
	enum VersionMapper {

		PRIMITIVE_SHORT((o) -> (short) o, (o) -> (short) (((short) o) + 1)), PRIMITIVE_INT((o) -> (int) o,
				(o) -> ((int) o) + 1), PRIMITIVE_LONG((o) -> (long) o, (o) -> ((long) o) + 1), SHORT(
						(o) -> o == null ? (short) 0 : (Short) o,
						(o) -> o == null ? (short) 1 : (short) (((Short) o) + 1)), INTEGER((o) -> o == null ? 0 : (Integer) o,
								(o) -> o == null ? 1 : ((Integer) o) + 1), LONG((o) -> o == null ? 0L : (Long) o,
										(o) -> o == null ? 1L : ((Long) o) + 1);

		private final Function<Object, Number> getFunction;
		private final Function<Object, Number> nextFunction;

		VersionMapper(Function<Object, Number> getFunction, Function<Object, Number> nextFunction) {

			this.getFunction = getFunction;
			this.nextFunction = nextFunction;
		}

		Number get(Object o) {
			return getFunction.apply(o);
		}

		Number next(Object o) {
			return nextFunction.apply(o);
		}

		static final Map<Class<?>, VersionMapper> BY_TYPE = new HashMap<>();

		static VersionMapper forType(Class<?> type) {
			if (BY_TYPE.isEmpty()) {
				initByTypeMap();
			}
			VersionMapper mapper = BY_TYPE.get(type);
			if (mapper == null) {
				throw new IllegalStateException(String.format("Invalid type for @Version field: %s.", type.getName()));
			}
			return mapper;
		}

		private static void initByTypeMap() {
			BY_TYPE.put(int.class, PRIMITIVE_INT);
			BY_TYPE.put(Integer.class, INTEGER);
			BY_TYPE.put(short.class, PRIMITIVE_SHORT);
			BY_TYPE.put(Short.class, SHORT);
			BY_TYPE.put(long.class, PRIMITIVE_LONG);
			BY_TYPE.put(Long.class, LONG);
		}

	}

}
