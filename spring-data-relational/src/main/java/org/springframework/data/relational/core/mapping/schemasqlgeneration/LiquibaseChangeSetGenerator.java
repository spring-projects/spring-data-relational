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

/**
 * Use this class to generate Liquibase change sets.
 *
 * First create a {@link SchemaModel} instance passing in a RelationalContext to have
 * a model that represents the Table(s)/Column(s) that the code expects to exist.
 *
 * And then optionally create a Liquibase database object that points to an existing database
 * if one desires to create a changeset that could be applied to that database.
 *
 * If a Liquibase database object is not used, then the change set created would be
 * something that could be applied to an empty database to make it match the state of the code.
 *
 * Prior to applying the changeset one should review and make adjustments appropriately.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class LiquibaseChangeSetGenerator {

    private final SchemaModel sourceModel;
    private final Database targetDatabase;

    /**
     * Use this to generate a ChangeSet that can be used on an empty database
     *
     * @author Kurt Niemi
     * @since 3.2
     *
     * @param sourceModel - Model representing table(s)/column(s) as existing in code
     */
    public LiquibaseChangeSetGenerator(SchemaModel sourceModel) {

        this.sourceModel = sourceModel;
        this.targetDatabase = null;
    }

    /**
     * Use this to generate a ChangeSet against an existing database
     *
     * @author Kurt Niemi
     * @since 3.2
     *
     * @param sourceModel - Model representing table(s)/column(s) as existing in code
     * @param targetDatabase - Existing Liquibase database
     */
    public LiquibaseChangeSetGenerator(SchemaModel sourceModel, Database targetDatabase) {

        this.sourceModel = sourceModel;
        this.targetDatabase = targetDatabase;
    }

    /**
     * Generates a Liquibase Changeset
     *
     * @author Kurt Niemi
     * @since 3.2
     *
     * @param changeLogFilePath - File that changeset will be written to (or append to an existing ChangeSet file)
     * @throws InvalidExampleException
     * @throws DatabaseException
     * @throws IOException
     * @throws ChangeLogParseException
     */
    public void generateLiquibaseChangeset(String changeLogFilePath) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        String changeSetId = Long.toString(System.currentTimeMillis());
        generateLiquibaseChangeset(changeLogFilePath, changeSetId, "Spring Data JDBC");
    }

    /**
     * Generates a Liquibase Changeset
     *
     * @author Kurt Niemi
     * @since 3.2
     *
     * @param changeLogFilePath - File that changeset will be written to (or append to an existing ChangeSet file)
     * @param changeSetId - A unique value to identify the changeset
     * @param changeSetAuthor - Author information to be written to changeset file.
     * @throws InvalidExampleException
     * @throws DatabaseException
     * @throws IOException
     * @throws ChangeLogParseException
     */
    public void generateLiquibaseChangeset(String changeLogFilePath, String changeSetId, String changeSetAuthor) throws InvalidExampleException, DatabaseException, IOException, ChangeLogParseException {

        SchemaDiff difference;

        if (targetDatabase != null) {
            SchemaModel liquibaseModel = getLiquibaseModel();
            difference = new SchemaDiff(sourceModel,liquibaseModel);
        } else {
            difference = new SchemaDiff(sourceModel, new SchemaModel());
        }

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

    private CreateTableChange createAddTableChange(TableModel table) {

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

    private DropTableChange createDropTableChange(TableModel table) {
        DropTableChange change = new DropTableChange();
        change.setSchemaName(table.getSchema());
        change.setTableName(table.getName().getReference());
        change.setCascadeConstraints(true);

        return change;
    }
}