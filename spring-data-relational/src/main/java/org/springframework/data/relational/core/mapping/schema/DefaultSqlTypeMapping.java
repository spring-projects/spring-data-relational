/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.relational.core.mapping.schema;

import java.util.HashMap;

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.ClassUtils;

/**
 * Class that provides a default implementation of mapping Java type to a Database type. To customize the mapping an
 * instance of a class implementing {@link SqlTypeMapping} interface can be set on the {@link Tables} class
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class DefaultSqlTypeMapping implements SqlTypeMapping {

	private final HashMap<Class<?>, String> mapClassToDatabaseType = new HashMap<>();

	public DefaultSqlTypeMapping() {

		mapClassToDatabaseType.put(String.class, "VARCHAR(255 BYTE)");
		mapClassToDatabaseType.put(Boolean.class, "TINYINT");
		mapClassToDatabaseType.put(Double.class, "DOUBLE");
		mapClassToDatabaseType.put(Float.class, "FLOAT");
		mapClassToDatabaseType.put(Integer.class, "INT");
		mapClassToDatabaseType.put(Long.class, "BIGINT");
	}

	@Override
	public String getColumnType(RelationalPersistentProperty property) {
		return mapClassToDatabaseType.get(ClassUtils.resolvePrimitiveIfNecessary(property.getActualType()));
	}
}
