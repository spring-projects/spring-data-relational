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
package org.springframework.data.relational.core.dialect;

/**
 * Interface declaring methods that express how a dialect supports array-typed columns.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public interface ArrayColumns {

	/**
	 * Returns {@literal true} if the dialect supports array-typed columns.
	 *
	 * @return {@literal true} if the dialect supports array-typed columns.
	 */
	boolean isSupported();

	/**
	 * Translate the {@link Class user type} of an array into the dialect-specific type. This method considers only the
	 * component type.
	 *
	 * @param userType component type of the array.
	 * @return the dialect-supported array type.
	 * @throws UnsupportedOperationException if array typed columns are not supported.
	 * @throws IllegalArgumentException if the {@code userType} is not a supported array type.
	 */
	Class<?> getArrayType(Class<?> userType);

	/**
	 * Default {@link ArrayColumns} implementation for dialects that do not support array-typed columns.
	 */
	enum Unsupported implements ArrayColumns {

		INSTANCE;

		@Override
		public boolean isSupported() {
			return false;
		}

		@Override
		public Class<?> getArrayType(Class<?> userType) {
			throw new UnsupportedOperationException("Array types not supported");
		}
	}

	/**
	 * Unwrap the nested {@link Class#getComponentType()} from a given {@link Class}.
	 *
	 * @param clazz the type to inspect.
	 * @return the unwrapped component type.
	 * @since 3.0
	 */
	static Class<?> unwrapComponentType(Class<?> clazz) {

		Class<?> componentType = clazz;
		while (componentType.isArray()) {
			componentType = componentType.getComponentType();
		}

		return componentType;
	}
}
