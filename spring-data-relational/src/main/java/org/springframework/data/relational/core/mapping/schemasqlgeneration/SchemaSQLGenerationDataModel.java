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
import liquibase.change.ColumnConfig;
import liquibase.change.core.CreateTableChange;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeLogChild;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.MockDatabaseConnection;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.core.yaml.YamlChangeLogParser;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.ChangeLogSerializerFactory;
import liquibase.serializer.SnapshotSerializer;
import liquibase.serializer.SnapshotSerializerFactory;
import liquibase.serializer.core.formattedsql.FormattedSqlChangeLogSerializer;
import liquibase.serializer.core.yaml.YamlChangeLogSerializer;
import liquibase.snapshot.*;
import liquibase.structure.DatabaseObject;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.relational.core.mapping.*;
import org.springframework.data.relational.core.sql.DefaultSqlIdentifier;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 * Model class that contains Table/Column information that can be used
 * to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class SchemaSQLGenerationDataModel {
    private final List<TableModel> tableData = new ArrayList<TableModel>();
    public BaseTypeMapper typeMapper;

    /**
     * Default constructor so that we can deserialize a model
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
                    entity.getPersistentProperties(Column.class).iterator();

            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                ColumnModel columnModel = new ColumnModel(p.getColumnName(),
                        typeMapper.databaseTypeFromClass(p.getActualType()),
                        true);
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

            System.out.println("Table " + table.getName().getReference() + " modified");
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

    public void persist(String fileName) throws IOException {
        FileOutputStream file = new FileOutputStream(fileName);
        ObjectOutputStream out = new ObjectOutputStream(file);
        out.writeObject(this);

        out.close();
        file.close();
    }

    public static SchemaSQLGenerationDataModel load(String fileName) throws IOException, ClassNotFoundException {
        FileInputStream file = new FileInputStream(fileName);
        ObjectInputStream in = new ObjectInputStream(file);

        SchemaSQLGenerationDataModel model = (SchemaSQLGenerationDataModel) in.readObject();
        return model;
    }

    public void generateLiquibaseChangeset(Database database, String changeLogFilePath) throws InvalidExampleException, DatabaseException, IOException {
        String changeSetId = Long.toString(System.currentTimeMillis());
        generateLiquibaseChangeset(database,changeLogFilePath, changeSetId, "Spring Data JDBC");
    }

    public void generateLiquibaseChangeset(Database database, String changeLogFilePath, String changeSetId, String changeSetAuthor) throws InvalidExampleException, DatabaseException, IOException {
        CatalogAndSchema[] schemas = new CatalogAndSchema[] { database.getDefaultSchema() };
        SnapshotControl snapshotControl = new SnapshotControl(database);

        DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, database, snapshotControl);
        Set<liquibase.structure.core.Table> tables = snapshot.get(liquibase.structure.core.Table.class);

        SchemaSQLGenerationDataModel liquibaseModel = new SchemaSQLGenerationDataModel();

        for (liquibase.structure.core.Table table : tables) {

            SqlIdentifier tableName = new DefaultSqlIdentifier(table.getName(), false);
            TableModel tableModel = new TableModel(table.getSchema().getCatalogName(), tableName);
            liquibaseModel.getTableData().add(tableModel);
            //System.out.println(table.getName());
            List<liquibase.structure.core.Column> columns = table.getColumns();
            for (liquibase.structure.core.Column column : columns) {
                //System.out.println("--- " + column.getName() + "," + column.getType());
            }
        }

        SchemaDiff difference = diffModel(liquibaseModel);

        File changeLogFile = new File(changeLogFilePath);

        ChangeLogSerializerFactory factory = ChangeLogSerializerFactory.getInstance();
        ChangeLogSerializer serializer = new YamlChangeLogSerializer();
        DatabaseChangeLog databaseChangeLog = new DatabaseChangeLog(changeLogFilePath);
        ChangeSet changeSet = new ChangeSet(changeSetId, changeSetAuthor, false, false, "", "", "" , databaseChangeLog);

        for (TableModel t : difference.getTableAdditions()) {
            System.out.println(t.getName().getReference() + " to be added.");
            CreateTableChange newTable = createAddTableChange(t);
            changeSet.addChange(newTable);
        }

        for (TableModel t : difference.getTableDeletions()) {
            System.out.println(t.getName().getReference() + " to be removed.");
            DropTableChange dropTable = createDropTableChange(t);
            changeSet.addChange(dropTable);
        }

        List changes = new ArrayList<ChangeLogChild>();
        changes.add(changeSet);
        FileOutputStream fos = new FileOutputStream(changeLogFile);
        serializer.write(changes, fos);

    }

    CreateTableChange createAddTableChange(TableModel table) {
        CreateTableChange change = new CreateTableChange();
        change.setSchemaName(table.getSchema());
        change.setTableName(table.getName().getReference());

        for (ColumnModel column : table.getColumns()) {
            ColumnConfig columnConfig = new ColumnConfig();
            columnConfig.setName(column.getName().getReference());
            columnConfig.setType(column.getType());
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
