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

import java.util.ArrayList;
import java.util.List;

/**
 * Class that models a Table for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class TableModel {
    private String schema;
    private String name;
    private List<ColumnModel> columns = new ArrayList<ColumnModel>();
    private List<ColumnModel> keyColumns = new ArrayList<ColumnModel>();
    private List<ForeignKeyColumnModel> foreignKeyColumns = new ArrayList<ForeignKeyColumnModel>();

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnModel> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnModel> columns) {
        this.columns = columns;
    }

    public List<ColumnModel> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<ColumnModel> keyColumns) {
        this.keyColumns = keyColumns;
    }

    public List<ForeignKeyColumnModel> getForeignKeyColumns() {
        return foreignKeyColumns;
    }

    public void setForeignKeyColumns(List<ForeignKeyColumnModel> foreignKeyColumns) {
        this.foreignKeyColumns = foreignKeyColumns;
    }
}
