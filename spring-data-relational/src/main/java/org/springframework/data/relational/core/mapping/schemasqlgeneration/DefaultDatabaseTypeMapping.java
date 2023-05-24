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
package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.HashMap;


/**
 * Class that provides a default implementation of mapping Java type to a Database type.
 *
 * To customize the mapping an instance of a class implementing {@link DatabaseTypeMapping} interface
 * can be set on the {@link SchemaModel} class
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class DefaultDatabaseTypeMapping implements DatabaseTypeMapping {

    final HashMap<Class<?>,String> mapClassToDatabaseType = new HashMap<Class<?>,String>();

    public DefaultDatabaseTypeMapping() {

        mapClassToDatabaseType.put(String.class, "VARCHAR(255 BYTE)");
        mapClassToDatabaseType.put(Boolean.class, "TINYINT");
        mapClassToDatabaseType.put(Double.class, "DOUBLE");
        mapClassToDatabaseType.put(Float.class, "FLOAT");
        mapClassToDatabaseType.put(Integer.class, "INT");
        mapClassToDatabaseType.put(Long.class, "BIGINT");
    }
    public String databaseTypeFromClass(Class<?> type) {

        return mapClassToDatabaseType.get(type);
    }
}
