/*
 * Copyright 2022-2024 the original author or authors.
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

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link ArrayColumns} support using the actual object type or {@link Class#isPrimitive() boxed primitives} Java types.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see ClassUtils#resolvePrimitiveIfNecessary
 */
public class ObjectArrayColumns implements ArrayColumns {

	public static final ObjectArrayColumns INSTANCE = new ObjectArrayColumns();

	@Override
	public boolean isSupported() {
		return true;
	}

	@Override
	public Class<?> getArrayType(Class<?> userType) {

		Assert.notNull(userType, "Array component type must not be null");

		return ClassUtils.resolvePrimitiveIfNecessary(ArrayColumns.unwrapComponentType(userType));
	}
}
