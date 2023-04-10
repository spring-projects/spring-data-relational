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

import java.io.Serial;
import java.io.Serializable;

/**
 * Class that models a Column for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class ColumnModel implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String name;
    private final Class<?> type;
    private final boolean nullable;

    public ColumnModel(String name, Class<?> type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public ColumnModel(String name, Class<?> type) {
        this.name = name;
        this.type = type;
        this.nullable = false;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }
}
