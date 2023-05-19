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

import liquibase.CatalogAndSchema;
import liquibase.change.AddColumnConfig;
import liquibase.change.ColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.CreateTableChange;
import liquibase.change.core.DropColumnChange;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeLogChild;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.DatabaseException;
import liquibase.parser.core.yaml.YamlChangeLogParser;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.core.yaml.YamlChangeLogSerializer;
import liquibase.snapshot.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.*;
import org.springframework.data.relational.core.sql.SqlIdentifier;

import java.io.*;
import java.util.*;

/**
 * Model class that contains Table/Column information that can be used
 * to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class SchemaSQLGenerationDataModel
{
    private final List<TableModel> tableData = new ArrayList<TableModel>();
    public BaseTypeMapper typeMapper;

    /**
     * Create empty model
     */
    public SchemaSQLGenerationDataModel() {

    }

    /**
     * Create model from a RelationalMappingContext
     */
    public SchemaSQLGenerationDataModel(RelationalMappingContext context) {

        if (typeMapper == null) {
            typeMapper = new BaseTypeMapper();
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
                        typeMapper.databaseTypeFromClass(p.getActualType()),
                        true, setIdentifierColumns.contains(p));
                tableModel.getColumns().add(columnModel);
            }

            tableData.add(tableModel);
        }
    }

    void diffTableAdditionDeletion(SchemaSQLGenerationDataModel source, SchemaDiff diff) {

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

    void diffTable(SchemaSQLGenerationDataModel source, SchemaDiff diff) {

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

    public SchemaDiff diffModel(SchemaSQLGenerationDataModel source) {

        SchemaDiff diff = new SchemaDiff();

        diffTableAdditionDeletion(source, diff);
        diffTable(source, diff);

        return diff;
    }

    public List<TableModel> getTableData() {
        return tableData;
    }

    public void generateLiquibaseChangeset(Database database, String changeLogFilePath) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        String changeSetId = Long.toString(System.currentTimeMillis());
        generateLiquibaseChangeset(database,changeLogFilePath, changeSetId, "Spring Data JDBC");
    }

    public void generateLiquibaseChangeset(Database database, String changeLogFilePath, String changeSetId, String changeSetAuthor) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        CatalogAndSchema[] schemas = new CatalogAndSchema[] { database.getDefaultSchema() };
        SnapshotControl snapshotControl = new SnapshotControl(database);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, database, snapshotControl);
        Set<liquibase.structure.core.Table> tables = snapshot.get(liquibase.structure.core.Table.class);

        SchemaSQLGenerationDataModel liquibaseModel = new SchemaSQLGenerationDataModel();

        for (TableModel t : tableData) {
            if (t.getSchema() == null || t.getSchema().isEmpty()) {
                t.setSchema(database.getDefaultSchema().getCatalogName());
            }
        }

        for (liquibase.structure.core.Table table : tables) {

            // Exclude internal Liquibase tables from comparison
            if (table.getName().startsWith("DATABASECHANGELOG")) {
                continue;
            }

            SqlIdentifier tableName = new DerivedSqlIdentifier(table.getName(), true);
            TableModel tableModel = new TableModel(table.getSchema().getCatalogName(), tableName);
            liquibaseModel.getTableData().add(tableModel);

            List<liquibase.structure.core.Column> columns = table.getColumns();
            for (liquibase.structure.core.Column column : columns) {
                SqlIdentifier columnName = new DerivedSqlIdentifier(column.getName(), true);
                String type = column.getType().toString();
                boolean nullable = column.isNullable();
                ColumnModel columnModel = new ColumnModel(columnName, type, nullable, false);
                tableModel.getColumns().add(columnModel);
            }
        }

        SchemaDiff difference = diffModel(liquibaseModel);

        File changeLogFile = new File(changeLogFilePath);

        DatabaseChangeLog databaseChangeLog;

        try {
            YamlChangeLogParser parser = new YamlChangeLogParser();
            DirectoryResourceAccessor resourceAccessor = new DirectoryResourceAccessor(changeLogFile.getParentFile());
            ChangeLogParameters parameters = new ChangeLogParameters();
            databaseChangeLog = parser.parse(changeLogFilePath, parameters, resourceAccessor);
        } catch (Exception ex) {
            databaseChangeLog = new DatabaseChangeLog(changeLogFilePath);
        }

        ChangeLogSerializer serializer = new YamlChangeLogSerializer();
        ChangeSet changeSet = new ChangeSet(changeSetId, changeSetAuthor, false, false, "", "", "" , databaseChangeLog);

        for (TableModel t : difference.getTableAdditions()) {
            CreateTableChange newTable = createAddTableChange(t);
            changeSet.addChange(newTable);
        }

        for (TableModel t : difference.getTableDeletions()) {
            DropTableChange dropTable = createDropTableChange(t);
            changeSet.addChange(dropTable);
        }

        for (TableDiff t : difference.getTableDiff()) {

            if (t.getAddedColumns().size() > 0) {
                AddColumnChange addColumnChange = new AddColumnChange();
                addColumnChange.setSchemaName(t.getTableModel().getSchema());
                addColumnChange.setTableName(t.getTableModel().getName().getReference());

                for (ColumnModel column : t.getAddedColumns()) {
                    AddColumnConfig addColumn = createAddColumnChange(column);
                    addColumnChange.addColumn(addColumn);
                }

                changeSet.addChange(addColumnChange);
            }

            if (t.getDeletedColumns().size() > 0) {
                DropColumnChange dropColumnChange = new DropColumnChange();
                dropColumnChange.setSchemaName(t.getTableModel().getSchema());
                dropColumnChange.setTableName(t.getTableModel().getName().getReference());

                List<ColumnConfig> dropColumns = new ArrayList<ColumnConfig>();
                for (ColumnModel column : t.getDeletedColumns()) {
                    ColumnConfig config = new ColumnConfig();
                    config.setName(column.getName().getReference());
                    dropColumns.add(config);
                }
                dropColumnChange.setColumns(dropColumns);
                changeSet.addChange(dropColumnChange);
            }
        }

        List changes = new ArrayList<ChangeLogChild>();
        for (ChangeSet change : databaseChangeLog.getChangeSets()) {
            changes.add(change);
        }
        changes.add(changeSet);
        FileOutputStream fos = new FileOutputStream(changeLogFile);
        serializer.write(changes, fos);
    }

    private AddColumnConfig createAddColumnChange(ColumnModel column) {

        AddColumnConfig config = new AddColumnConfig();
        config.setName(column.getName().getReference());
        config.setType(column.getType());

        if (column.isIdentityColumn()) {
            config.setAutoIncrement(true);
        }
        return config;
    }

    CreateTableChange createAddTableChange(TableModel table) {

        CreateTableChange change = new CreateTableChange();
        change.setSchemaName(table.getSchema());
        change.setTableName(table.getName().getReference());

        for (ColumnModel column : table.getColumns()) {
            ColumnConfig columnConfig = new ColumnConfig();
            columnConfig.setName(column.getName().getReference());
            columnConfig.setType(column.getType());

            if (column.isIdentityColumn()) {
                columnConfig.setAutoIncrement(true);
                ConstraintsConfig constraints =  new ConstraintsConfig();
                constraints.setPrimaryKey(true);
                columnConfig.setConstraints(constraints);
            }
            change.addColumn(columnConfig);
        }

        return change;
    }

    DropTableChange createDropTableChange(TableModel table) {
        DropTableChange change = new DropTableChange();
        change.setSchemaName(table.getSchema());
        change.setTableName(table.getName().getReference());
        change.setCascadeConstraints(true);

        return change;
    }
}
