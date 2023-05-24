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

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.*;

import java.util.*;

/**
 * Model class that contains Table/Column information that can be used
 * to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class SchemaModel
{
    private final List<TableModel> tableData = new ArrayList<TableModel>();
    public DatabaseTypeMapping databaseTypeMapping;

    /**
     * Create empty model
     */
    public SchemaModel() {

    }

    /**
     * Create model from a RelationalMappingContext
     */
    public SchemaModel(RelationalMappingContext context) {

        if (databaseTypeMapping == null) {
            databaseTypeMapping = new DefaultDatabaseTypeMapping();
        }

        for (RelationalPersistentEntity entity : context.getPersistentEntities()) {
            TableModel tableModel = new TableModel(entity.getTableName());

            Iterator<BasicRelationalPersistentProperty> iter =
                    entity.getPersistentProperties(Id.class).iterator();
            Set<BasicRelationalPersistentProperty> setIdentifierColumns = new HashSet<BasicRelationalPersistentProperty>();
            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                setIdentifierColumns.add(p);
            }

            iter =
                    entity.getPersistentProperties(Column.class).iterator();

            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                ColumnModel columnModel = new ColumnModel(p.getColumnName(),
                        databaseTypeMapping.databaseTypeFromClass(p.getActualType()),
                        true, setIdentifierColumns.contains(p));
                tableModel.getColumns().add(columnModel);
            }

            tableData.add(tableModel);
        }
    }

    void diffTableAdditionDeletion(SchemaModel source, SchemaDiff diff) {

        Set<TableModel> sourceTableData = new HashSet<TableModel>(source.getTableData());
        Set<TableModel> targetTableData = new HashSet<TableModel>(getTableData());

        // Identify deleted tables
        Set<TableModel> deletedTables = new HashSet<TableModel>(sourceTableData);
        deletedTables.removeAll(targetTableData);
        diff.getTableDeletions().addAll(deletedTables);

        // Identify added tables
        Set<TableModel> addedTables = new HashSet<TableModel>(targetTableData);
        addedTables.removeAll(sourceTableData);
        diff.getTableAdditions().addAll(addedTables);
    }

    void diffTable(SchemaModel source, SchemaDiff diff) {

        HashMap<String, TableModel> sourceTablesMap = new HashMap<String,TableModel>();
        for (TableModel table : source.getTableData()) {
            sourceTablesMap.put(table.getSchema() + "." + table.getName().getReference(), table);
        }

        Set<TableModel> existingTables = new HashSet<TableModel>(getTableData());
        existingTables.removeAll(diff.getTableAdditions());

        for (TableModel table : existingTables) {
            TableDiff tableDiff = new TableDiff(table);
            diff.getTableDiff().add(tableDiff);

            TableModel sourceTable = sourceTablesMap.get(table.getSchema() + "." + table.getName().getReference());

            Set<ColumnModel> sourceTableData = new HashSet<ColumnModel>(sourceTable.getColumns());
            Set<ColumnModel> targetTableData = new HashSet<ColumnModel>(table.getColumns());

            // Identify deleted columns
            Set<ColumnModel> deletedColumns = new HashSet<ColumnModel>(sourceTableData);
            deletedColumns.removeAll(targetTableData);

            tableDiff.getDeletedColumns().addAll(deletedColumns);

            // Identify added columns
            Set<ColumnModel> addedColumns = new HashSet<ColumnModel>(targetTableData);
            addedColumns.removeAll(sourceTableData);
            tableDiff.getAddedColumns().addAll(addedColumns);
        }
    }

    public SchemaDiff diffModel(SchemaModel source) {

        SchemaDiff diff = new SchemaDiff();

        diffTableAdditionDeletion(source, diff);
        diffTable(source, diff);

        return diff;
    }

    public List<TableModel> getTableData() {
        return tableData;
    }
}
