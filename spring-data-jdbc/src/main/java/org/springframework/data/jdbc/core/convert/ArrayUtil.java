/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import lombok.experimental.UtilityClass;

/**
 * A collection of utility methods for dealing with arrays.
 *
 * @author Jens Schauder
 * @since 1.1
 */
@UtilityClass
class ArrayUtil {

	/**
	 * Converts an {@code Byte[]} into a {@code byte[]}.
	 *
	 * @param byteArray the array to be converted. Must not be {@literal null}.
	 * @return a {@code byte[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static byte[] toPrimitiveByteArray(Byte[] byteArray) {

		byte[] bytes = new byte[byteArray.length];
		for (int i = 0; i < byteArray.length; i++) {
			bytes[i] = byteArray[i];
		}
		return bytes;
	}
}
