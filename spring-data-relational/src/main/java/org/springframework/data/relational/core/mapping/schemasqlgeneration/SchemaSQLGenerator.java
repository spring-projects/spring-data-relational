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

import org.springframework.data.relational.core.sql.IdentifierProcessing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SchemaSQLGenerator {

    private final IdentifierProcessing identifierProcssing;
    public SchemaSQLGenerator(IdentifierProcessing identifierProcessing) {
        this.identifierProcssing = identifierProcessing;
    }

    List<List<TableModel>> reorderTablesInHierarchy(SchemaSQLGenerationDataModel dataModel) {

        // ::TODO:: Take Parent/Child relationships into account (i.e if a child table has
        // a Foreign Key to a table, that parent table needs to be created first.

        // For now this method will simple put the tables in the same level
        List<List<TableModel>> orderedTables = new ArrayList<List<TableModel>>();
        List<TableModel> tables = new ArrayList<TableModel>();

        for (TableModel table : dataModel.getTableData()) {
            tables.add(table);
        }
        orderedTables.add(tables);

        return orderedTables;
    }

    HashMap<Class<?>,String> mapClassToSQLType = null;

    public String generateSQL(ColumnModel column) {

        StringBuilder sql = new StringBuilder();
        sql.append(column.getName().toSql(identifierProcssing));
        sql.append(" ");

        sql.append(column.getType());

        if (!column.isNullable()) {
            sql.append(" NOT NULL");
        }

        return sql.toString();
    }

    public String generatePrimaryKeySQL(TableModel table) {
        // ::TODO:: Implement
        return "";
    }

    public String generateForeignKeySQL(TableModel table) {
        // ::TODO:: Implement
        return "";
    }

    public String generateSQL(TableModel table) {

        StringBuilder sql = new StringBuilder();

        sql.append("CREATE TABLE ");
        sql.append(table.getName().toSql(identifierProcssing));
        sql.append(" (");

        int numColumns = table.getColumns().size();
        for (int i=0; i < numColumns; i++) {
            sql.append(generateSQL(table.getColumns().get(i)));
            if (i != numColumns-1) {
                sql.append(",");
            }
        }

        sql.append(generatePrimaryKeySQL(table));
        sql.append(generateForeignKeySQL(table));

        sql.append(" );");

        return sql.toString();
    }

    public String generateSQL(SchemaSQLGenerationDataModel dataModel) {

        StringBuilder sql = new StringBuilder();
        List<List<TableModel>> orderedTables = reorderTablesInHierarchy(dataModel);

        for (List<TableModel> tables : orderedTables) {
            for (TableModel table : tables) {
                String tableSQL = generateSQL(table);
                sql.append(tableSQL + "\n");
            }
        }

        return sql.toString();
    }
}
