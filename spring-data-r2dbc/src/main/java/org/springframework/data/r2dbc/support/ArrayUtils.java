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
package org.springframework.data.r2dbc.support;

import java.lang.reflect.Array;

import org.springframework.util.Assert;

/**
 * Utilities for array interaction.
 *
 * @author Mark Paluch
 */
public abstract class ArrayUtils {

	/**
	 * Determine the number of dimensions for an array object.
	 *
	 * @param value the array to inspect, must not be {@literal null}.
	 * @return number of dimensions.
	 */
	public static int getDimensionDepth(Object value) {

		Assert.notNull(value, "Value must not be null");

		return getDimensionDepth(value.getClass());
	}

	/**
	 * Determine the number of dimensions for an {@code arrayClass}.
	 *
	 * @param arrayClass the array type to inspect, must not be {@literal null}.
	 * @return number of dimensions.
	 */
	public static int getDimensionDepth(Class<?> arrayClass) {

		Assert.isTrue(arrayClass != null && arrayClass.isArray(), "Array class must be an array");

		int result = 0;
		Class<?> type = arrayClass;

		while (type.isArray()) {
			result++;

			type = type.getComponentType();
		}

		return result;
	}

	/**
	 * Create a new empty array with the given number of {@code dimensions}.
	 *
	 * @param componentType array component type.
	 * @param dimensions number of dimensions (depth).
	 * @return a new empty array with the given number of {@code dimensions}.
	 */
	public static Class<?> getArrayClass(Class<?> componentType, int dimensions) {

		Assert.notNull(componentType, "Component type must not be null");

		return Array.newInstance(componentType, new int[dimensions]).getClass();
	}

	/**
	 * Utility constructor.
	 */
	private ArrayUtils() {

	}
}
