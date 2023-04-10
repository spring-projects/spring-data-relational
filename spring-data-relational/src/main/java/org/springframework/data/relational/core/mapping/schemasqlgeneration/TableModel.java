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

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that models a Table for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class TableModel implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String schema;
    private final SqlIdentifier name;
    private final List<ColumnModel> columns = new ArrayList<ColumnModel>();
    private final List<ColumnModel> keyColumns = new ArrayList<ColumnModel>();
    private final List<ForeignKeyColumnModel> foreignKeyColumns = new ArrayList<ForeignKeyColumnModel>();

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

    public SqlIdentifier getName() {
        return name;
    }

    public List<ColumnModel> getColumns() {
        return columns;
    }

    public List<ColumnModel> getKeyColumns() {
        return keyColumns;
    }

    public List<ForeignKeyColumnModel> getForeignKeyColumns() {
        return foreignKeyColumns;
    }
}
