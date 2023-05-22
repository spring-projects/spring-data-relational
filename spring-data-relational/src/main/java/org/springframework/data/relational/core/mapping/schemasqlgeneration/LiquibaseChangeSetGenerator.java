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
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import org.springframework.data.relational.core.mapping.DerivedSqlIdentifier;
import org.springframework.data.relational.core.sql.SqlIdentifier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LiquibaseChangeSetGenerator {

    private final SchemaModel sourceModel;
    private final Database targetDatabase;

    public LiquibaseChangeSetGenerator(SchemaModel sourceModel, Database targetDatabase) {

        this.sourceModel = sourceModel;
        this.targetDatabase = targetDatabase;
    }

    public void generateLiquibaseChangeset(String changeLogFilePath) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        String changeSetId = Long.toString(System.currentTimeMillis());
        generateLiquibaseChangeset(changeLogFilePath, changeSetId, "Spring Data JDBC");
    }

    public void generateLiquibaseChangeset(String changeLogFilePath, String changeSetId, String changeSetAuthor) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        SchemaModel liquibaseModel = getLiquibaseModel();
        SchemaDiff difference = sourceModel.diffModel(liquibaseModel);

        DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogFilePath);

        ChangeSet changeSet = new ChangeSet(changeSetId, changeSetAuthor, false, false, "", "", "" , databaseChangeLog);

        generateTableAdditionsDeletions(changeSet, difference);
        generateTableModifications(changeSet, difference);


        File changeLogFile = new File(changeLogFilePath);
        writeChangeSet(databaseChangeLog, changeSet, changeLogFile);
    }

    private void generateTableAdditionsDeletions(ChangeSet changeSet, SchemaDiff difference) {

        for (TableModel table : difference.getTableAdditions()) {
            CreateTableChange newTable = createAddTableChange(table);
            changeSet.addChange(newTable);
        }

        for (TableModel table : difference.getTableDeletions()) {
            DropTableChange dropTable = createDropTableChange(table);
            changeSet.addChange(dropTable);
        }
    }

    private void generateTableModifications(ChangeSet changeSet, SchemaDiff difference) {

        for (TableDiff table : difference.getTableDiff()) {

            if (table.getAddedColumns().size() > 0) {
                AddColumnChange addColumnChange = new AddColumnChange();
                addColumnChange.setSchemaName(table.getTableModel().getSchema());
                addColumnChange.setTableName(table.getTableModel().getName().getReference());

                for (ColumnModel column : table.getAddedColumns()) {
                    AddColumnConfig addColumn = createAddColumnChange(column);
                    addColumnChange.addColumn(addColumn);
                }

                changeSet.addChange(addColumnChange);
            }

            if (table.getDeletedColumns().size() > 0) {
                DropColumnChange dropColumnChange = new DropColumnChange();
                dropColumnChange.setSchemaName(table.getTableModel().getSchema());
                dropColumnChange.setTableName(table.getTableModel().getName().getReference());

                List<ColumnConfig> dropColumns = new ArrayList<ColumnConfig>();
                for (ColumnModel column : table.getDeletedColumns()) {
                    ColumnConfig config = new ColumnConfig();
                    config.setName(column.getName().getReference());
                    dropColumns.add(config);
                }
                dropColumnChange.setColumns(dropColumns);
                changeSet.addChange(dropColumnChange);
            }
        }
    }

    private DatabaseChangeLog getDatabaseChangeLog(String changeLogFilePath) {

        File changeLogFile = new File(changeLogFilePath);
        DatabaseChangeLog databaseChangeLog = null;

        try {
            YamlChangeLogParser parser = new YamlChangeLogParser();
            DirectoryResourceAccessor resourceAccessor = new DirectoryResourceAccessor(changeLogFile.getParentFile());
            ChangeLogParameters parameters = new ChangeLogParameters();
            databaseChangeLog = parser.parse(changeLogFilePath, parameters, resourceAccessor);
        } catch (Exception ex) {
            databaseChangeLog = new DatabaseChangeLog(changeLogFilePath);
        }
        return databaseChangeLog;
    }
    private void writeChangeSet(DatabaseChangeLog databaseChangeLog, ChangeSet changeSet, File changeLogFile) throws FileNotFoundException, IOException {

        ChangeLogSerializer serializer = new YamlChangeLogSerializer();
        List changes = new ArrayList<ChangeLogChild>();
        for (ChangeSet change : databaseChangeLog.getChangeSets()) {
            changes.add(change);
        }
        changes.add(changeSet);
        FileOutputStream fos = new FileOutputStream(changeLogFile);
        serializer.write(changes, fos);
    }

    private SchemaModel getLiquibaseModel() throws DatabaseException, InvalidExampleException {
        SchemaModel liquibaseModel = new SchemaModel();

        CatalogAndSchema[] schemas = new CatalogAndSchema[] { targetDatabase.getDefaultSchema() };
        SnapshotControl snapshotControl = new SnapshotControl(targetDatabase);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, targetDatabase, snapshotControl);
        Set<Table> tables = snapshot.get(liquibase.structure.core.Table.class);

        for (TableModel t : sourceModel.getTableData()) {
            if (t.getSchema() == null || t.getSchema().isEmpty()) {
                t.setSchema(targetDatabase.getDefaultSchema().getCatalogName());
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

            List<Column> columns = table.getColumns();
            for (liquibase.structure.core.Column column : columns) {
                SqlIdentifier columnName = new DerivedSqlIdentifier(column.getName(), true);
                String type = column.getType().toString();
                boolean nullable = column.isNullable();
                ColumnModel columnModel = new ColumnModel(columnName, type, nullable, false);
                tableModel.getColumns().add(columnModel);
            }
        }

        return liquibaseModel;
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
