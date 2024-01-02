/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping.schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.UUID;

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.ClassUtils;

/**
 * Class that provides a default implementation of mapping Java type to a Database type. To customize the mapping an
 * instance of a class implementing {@link SqlTypeMapping} interface can be set on the {@link Tables} class
 *
 * @author Kurt Niemi
 * @author Evgenii Koba
 * @author Jens Schauder
 * @since 3.2
 */
public class DefaultSqlTypeMapping implements SqlTypeMapping {

	private final HashMap<Class<?>, String> typeMap = new HashMap<>();

	public DefaultSqlTypeMapping() {

		typeMap.put(String.class, "VARCHAR(255 BYTE)");
		typeMap.put(Boolean.class, "TINYINT");
		typeMap.put(Double.class, "DOUBLE");
		typeMap.put(Float.class, "FLOAT");
		typeMap.put(Integer.class, "INT");
		typeMap.put(Long.class, "BIGINT");

		typeMap.put(BigInteger.class, "BIGINT");
		typeMap.put(BigDecimal.class, "NUMERIC");

		typeMap.put(UUID.class, "UUID");

		typeMap.put(LocalDate.class, "DATE");
		typeMap.put(LocalTime.class, "TIME");
		typeMap.put(LocalDateTime.class, "TIMESTAMP");

		typeMap.put(ZonedDateTime.class, "TIMESTAMPTZ");
	}

	@Override
	public String getColumnType(RelationalPersistentProperty property) {
		return getColumnType(property.getActualType());
	}

	@Override
	public String getColumnType(Class<?> type) {
		return typeMap.get(ClassUtils.resolvePrimitiveIfNecessary(type));
	}
}
