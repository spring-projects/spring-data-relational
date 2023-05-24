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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class that models a Table for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class TableModel {
    private String schema;
    private SqlIdentifier name;
    private final List<ColumnModel> columns = new ArrayList<ColumnModel>();
    private final List<ColumnModel> keyColumns = new ArrayList<ColumnModel>();

    public TableModel(String schema, SqlIdentifier name) {

        this.schema = schema;
        this.name = name;
    }
    public TableModel(SqlIdentifier name) {

        this(null, name);
    }

    public String getSchema() {

        return schema;
    }

    public void setSchema(String schema) {

        this.schema = schema;
    }

    public SqlIdentifier getName() {

        return name;
    }

    public void setName(SqlIdentifier name) {

        this.name = name;
    }

    public List<ColumnModel> getColumns() {

        return columns;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableModel that = (TableModel) o;

        // If we are missing the schema for either TableModel we will not treat that as being different
        if (schema != null && that.schema != null && !schema.isEmpty() && !that.schema.isEmpty()) {
            if (!Objects.equals(schema, that.schema)) {
                return false;
            }
        }
        if (!name.getReference().toUpperCase().equals(that.name.getReference().toUpperCase())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {

        return Objects.hash(name.getReference().toUpperCase());
    }
}
