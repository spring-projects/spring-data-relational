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

import org.springframework.data.relational.core.sql.SqlIdentifier;

import java.util.Objects;


/**
 * Class that models a Column for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class ColumnModel {
    private final SqlIdentifier name;
    private final String type;
    private final boolean nullable;
    private final boolean identityColumn;

    public ColumnModel(SqlIdentifier name, String type, boolean nullable, boolean identityColumn) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.identityColumn = identityColumn;
    }

    public ColumnModel(SqlIdentifier name, String type) {
        this.name = name;
        this.type = type;
        this.nullable = false;
        this.identityColumn = false;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isIdentityColumn() {
        return identityColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnModel that = (ColumnModel) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
