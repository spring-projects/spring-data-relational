/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.lang.reflect.Array;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;

/**
 * Utility to obtain {@link JdbcValue} instances for string values, collections, and arrays for string-based query
 * usage.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public abstract class JdbcValueBindUtil {

	private JdbcValueBindUtil() {}

	/**
	 * Obtains a {@link JdbcValue} for the given {@code value} and {@link JdbcParameters.JdbcParameter} to be bound to a
	 * query.
	 *
	 * @param converter
	 * @param value
	 * @param parameter
	 * @return
	 */
	public static JdbcValue getBindValue(JdbcConverter converter, @Nullable Object value,
			JdbcParameters.JdbcParameter parameter) {
		return getBindValue(converter, value, parameter.getTypeInformation(), parameter.getSqlType(),
				parameter.getActualSqlType());
	}

	private static JdbcValue getBindValue(JdbcConverter converter, @Nullable Object value,
			TypeInformation<?> typeInformation, SQLType sqlType, SQLType actualSqlType) {

		if (value == null) {
			return JdbcValue.of(value, sqlType);
		}

		if (typeInformation.isCollectionLike() && value instanceof Collection<?> collection) {

			TypeInformation<?> actualType = typeInformation.getActualType();

			// allow tuple-binding for collection of byte arrays to be used as BINARY,
			// we do not want to convert to column arrays.
			if (actualType != null && actualType.getType().isArray() && !actualType.getType().equals(byte[].class)) {

				TypeInformation<?> nestedElementType = actualType.getRequiredActualType();
				return writeCollection(collection, actualSqlType,
						array -> writeArrayValue(converter, actualSqlType, array, nestedElementType));
			}

			// parameter expansion
			return writeCollection(collection, actualSqlType,
					it -> converter.writeJdbcValue(it, typeInformation.getRequiredActualType(), actualSqlType));
		}

		return converter.writeJdbcValue(value, typeInformation, sqlType);
	}

	private static JdbcValue writeCollection(Collection<?> value, SQLType defaultType,
			Function<Object, Object> mapper) {

		if (value.isEmpty()) {
			return JdbcValue.of(value, defaultType);
		}

		JdbcValue jdbcValue;
		List<Object> mapped = new ArrayList<>(value.size());
		SQLType jdbcType = null;

		for (Object o : value) {

			Object mappedValue = mapper.apply(o);

			if (mappedValue instanceof JdbcValue jv) {
				if (jdbcType == null) {
					jdbcType = jv.getJdbcType();
				}
				mappedValue = jv.getValue();
			}

			mapped.add(mappedValue);
		}

		jdbcValue = JdbcValue.of(mapped, jdbcType == null ? defaultType : jdbcType);

		return jdbcValue;
	}

	private static JdbcValue writeArrayValue(JdbcConverter converter, SQLType actualSqlType, Object array,
			TypeInformation<?> nestedElementType) {

		int length = Array.getLength(array);
		Object[] mappedArray = new Object[length];
		SQLType sqlType = null;

		for (int i = 0; i < length; i++) {

			Object element = Array.get(array, i);
			JdbcValue converted = converter.writeJdbcValue(element, nestedElementType, actualSqlType);

			if (sqlType == null && converted.getJdbcType() != null) {
				sqlType = converted.getJdbcType();
			}
			mappedArray[i] = converted.getValue();
		}

		if (sqlType == null) {
			sqlType = JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(nestedElementType.getType()));
		}

		return JdbcValue.of(mappedArray, sqlType);
	}
}
