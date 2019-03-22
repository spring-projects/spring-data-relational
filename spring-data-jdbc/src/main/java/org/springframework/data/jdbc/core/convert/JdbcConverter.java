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

import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * A {@link JdbcConverter} is responsible for converting for values to the native relational representation and vice
 * versa.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public interface JdbcConverter extends RelationalConverter {

	/**
	 * Convert a property value into a {@link JdbcValue} that contains the converted value and information how to bind
	 * it to JDBC parameters.
	 *
	 * @param value a value as it is used in the object model. May be {@code null}.
	 * @param type {@link TypeInformation} into which the value is to be converted. Must not be {@code null}.
	 * @param sqlType the type constant from {@link java.sql.Types} to be used if non is specified by a converter.
	 * @return The converted value wrapped in a {@link JdbcValue}. Guaranteed to be not {@literal null}.
	 */
	JdbcValue writeJdbcValue(@Nullable Object value, Class<?> type, int sqlType);
}
