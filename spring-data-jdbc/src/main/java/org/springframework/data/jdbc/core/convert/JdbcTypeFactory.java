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
package org.springframework.data.jdbc.core.convert;

import java.sql.Array;

/**
 * Allows the creation of instances of database dependent types, e.g. {@link Array}.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public interface JdbcTypeFactory {

	/**
	 * An implementation used in places where a proper {@code JdbcTypeFactory} can not be provided but an instance needs
	 * to be provided anyway, mostly for providing backward compatibility. Calling it will result in an exception. The
	 * features normally supported by a {@link JdbcTypeFactory} will not work.
	 */
	static JdbcTypeFactory unsupported() {

		return value -> {
			throw new UnsupportedOperationException("This JdbcTypeFactory does not support Array creation");
		};
	}

	/**
	 * Converts the provided value in a {@link Array} instance.
	 *
	 * @param value the value to be converted. Must not be {@literal null}.
	 * @return an {@link Array}. Guaranteed to be not {@literal null}.
	 */
	Array createArray(Object[] value);
}
