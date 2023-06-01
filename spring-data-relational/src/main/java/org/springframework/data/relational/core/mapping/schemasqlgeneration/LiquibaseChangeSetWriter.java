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
import liquibase.exception.LiquibaseException;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.core.yaml.YamlChangeLogParser;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.core.yaml.YamlChangeLogSerializer;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Use this class to write Liquibase change sets.
 * <p>
 * First create a {@link MappedTables} instance passing in a RelationalContext to have a model that represents the
 * Table(s)/Column(s) that the code expects to exist. And then optionally create a Liquibase database object that points
 * to an existing database if one desires to create a changeset that could be applied to that database. If a database
 * object is not used, then the change set created would be something that could be applied to an empty database to make
 * it match the state of the code. Prior to applying the changeset one should review and make adjustments appropriately.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class LiquibaseChangeSetWriter {

	private final MappedTables sourceModel;
	private final Database targetDatabase;

	private ChangeLogSerializer changeLogSerializer = new YamlChangeLogSerializer();

	private ChangeLogParser changeLogParser = new YamlChangeLogParser();

	/**
	 * Predicate to identify Liquibase system tables.
	 */
	private final Predicate<String> liquibaseTables = table -> table.toUpperCase(Locale.ROOT)
			.startsWith("DATABASECHANGELOG");

	/**
	 * Filter predicate used to determine whether an existing table should be removed. Defaults to {@code false} to keep
	 * existing tables.
	 */
	public Predicate<String> dropTableFilter = table -> true;

	/**
	 * Filter predicate used to determine whether an existing column should be removed. Defaults to {@code false} to keep
	 * existing columns.
	 */
	public BiPredicate<String, String> dropColumnFilter = (table, column) -> false;

	/**
	 * Use this to generate a ChangeSet that can be used on an empty database
	 *
	 * @param sourceModel - Model representing table(s)/column(s) as existing in code
	 */
	public LiquibaseChangeSetWriter(MappedTables sourceModel) {

		this.sourceModel = sourceModel;
		this.targetDatabase = null;
	}

	/**
	 * Use this to generate a ChangeSet against an existing database
	 *
	 * @param sourceModel model representing table(s)/column(s) as existing in code.
	 * @param targetDatabase existing Liquibase database.
	 */
	public LiquibaseChangeSetWriter(MappedTables sourceModel, Database targetDatabase) {

		this.sourceModel = sourceModel;
		this.targetDatabase = targetDatabase;
	}

	/**
	 * Set the {@link ChangeLogSerializer}.
	 *
	 * @param changeLogSerializer
	 */
	public void setChangeLogSerializer(ChangeLogSerializer changeLogSerializer) {

		Assert.notNull(changeLogSerializer, "ChangeLogSerializer must not be null");

		this.changeLogSerializer = changeLogSerializer;
	}

	/**
	 * Set the {@link ChangeLogParser}.
	 *
	 * @param changeLogParser
	 */
	public void setChangeLogParser(ChangeLogParser changeLogParser) {

		Assert.notNull(changeLogParser, "ChangeLogParser must not be null");

		this.changeLogParser = changeLogParser;
	}

	/**
	 * Set the filter predicate to identify tables to drop. The predicate accepts the table name. Returning {@code true}
	 * will delete the table; {@code false} retains the table.
	 *
	 * @param dropTableFilter must not be {@literal null}.
	 */
	public void setDropTableFilter(Predicate<String> dropTableFilter) {

		Assert.notNull(dropTableFilter, "Drop Column filter must not be null");

		this.dropTableFilter = dropTableFilter;
	}

	/**
	 * Set the filter predicate to identify columns within a table to drop. The predicate accepts the table- and column
	 * name. Returning {@code true} will delete the column; {@code false} retains the column.
	 *
	 * @param dropColumnFilter must not be {@literal null}.
	 */
	public void setDropColumnFilter(BiPredicate<String, String> dropColumnFilter) {

		Assert.notNull(dropColumnFilter, "Drop Column filter must not be null");

		this.dropColumnFilter = dropColumnFilter;
	}

	/**
	 * Write a Liquibase changeset.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file). The
	 *          resource must resolve to a valid {@link Resource#getFile()}.
	 * @throws LiquibaseException
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource) throws LiquibaseException, IOException {

		String changeSetId = Long.toString(System.currentTimeMillis());
		writeChangeSet(changeLogResource, changeSetId, "Spring Data Relational");
	}

	/**
	 * Write a Liquibase changeset.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file).
	 * @param changeSetId unique value to identify the changeset.
	 * @param changeSetAuthor author information to be written to changeset file.
	 * @throws LiquibaseException
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource, String changeSetId, String changeSetAuthor)
			throws LiquibaseException, IOException {

		SchemaDiff difference;

		if (targetDatabase != null) {
			MappedTables liquibaseModel = getLiquibaseModel(targetDatabase);
			difference = new SchemaDiff(sourceModel, liquibaseModel);
		} else {
			difference = new SchemaDiff(sourceModel, new MappedTables());
		}

		DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogResource.getFile());
		ChangeSet changeSet = new ChangeSet(changeSetId, changeSetAuthor, false, false, "", "", "", databaseChangeLog);

		generateTableAdditionsDeletions(changeSet, difference);
		generateTableModifications(changeSet, difference);

		// File changeLogFile = new File(changeLogFilePath);
		writeChangeSet(databaseChangeLog, changeSet, changeLogResource.getFile());
	}

	private DatabaseChangeLog getDatabaseChangeLog(File changeLogFile) {

		DatabaseChangeLog databaseChangeLog;

		try {
			File parentDirectory = changeLogFile.getParentFile();
			if (parentDirectory == null) {
				parentDirectory = new File("./");
			}
			DirectoryResourceAccessor resourceAccessor = new DirectoryResourceAccessor(parentDirectory);
			ChangeLogParameters parameters = new ChangeLogParameters();
			databaseChangeLog = changeLogParser.parse(changeLogFile.getName(), parameters, resourceAccessor);
		} catch (Exception ex) {
			databaseChangeLog = new DatabaseChangeLog(changeLogFile.getAbsolutePath());
		}

		return databaseChangeLog;
	}

	private void generateTableAdditionsDeletions(ChangeSet changeSet, SchemaDiff difference) {

		for (TableModel table : difference.getTableAdditions()) {
			CreateTableChange newTable = changeTable(table);
			changeSet.addChange(newTable);
		}

		for (TableModel table : difference.getTableDeletions()) {
			// Do not delete/drop table if it is an external application table
			if (dropTableFilter.test(table.name())) {
				changeSet.addChange(dropTable(table));
			}
		}
	}

	private void generateTableModifications(ChangeSet changeSet, SchemaDiff difference) {

		for (TableDiff table : difference.getTableDiff()) {

			if (!table.columnsToAdd().isEmpty()) {
				changeSet.addChange(addColumns(table));
			}

			List<ColumnModel> deletedColumns = getColumnsToDrop(table);

			if (deletedColumns.size() > 0) {
				changeSet.addChange(dropColumns(table, deletedColumns));
			}
		}
	}

	private List<ColumnModel> getColumnsToDrop(TableDiff table) {

		List<ColumnModel> deletedColumns = new ArrayList<>();
		for (ColumnModel columnModel : table.columnsToDrop()) {

			if (dropColumnFilter.test(table.table().name(), columnModel.name())) {
				deletedColumns.add(columnModel);
			}
		}
		return deletedColumns;
	}

	private void writeChangeSet(DatabaseChangeLog databaseChangeLog, ChangeSet changeSet, File changeLogFile)
			throws IOException {

		List<ChangeLogChild> changes = new ArrayList<>(databaseChangeLog.getChangeSets());
		changes.add(changeSet);

		try (FileOutputStream fos = new FileOutputStream(changeLogFile)) {
			changeLogSerializer.write(changes, fos);
		}
	}

	private MappedTables getLiquibaseModel(Database targetDatabase) throws LiquibaseException {

		MappedTables liquibaseModel = new MappedTables();

		CatalogAndSchema[] schemas = new CatalogAndSchema[] { targetDatabase.getDefaultSchema() };
		SnapshotControl snapshotControl = new SnapshotControl(targetDatabase);

		DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, targetDatabase,
				snapshotControl);
		Set<Table> tables = snapshot.get(liquibase.structure.core.Table.class);
		List<TableModel> processed = associateTablesWithSchema(sourceModel.getTableData(), targetDatabase);

		sourceModel.getTableData().clear();
		sourceModel.getTableData().addAll(processed);

		for (liquibase.structure.core.Table table : tables) {

			// Exclude internal Liquibase tables from comparison
			if (liquibaseTables.test(table.getName())) {
				continue;
			}

			TableModel tableModel = new TableModel(table.getSchema().getCatalogName(), table.getName());

			List<Column> columns = table.getColumns();

			for (liquibase.structure.core.Column column : columns) {

				String type = column.getType().toString();
				boolean nullable = column.isNullable();
				ColumnModel columnModel = new ColumnModel(column.getName(), type, nullable, false);

				tableModel.columns().add(columnModel);
			}

			liquibaseModel.getTableData().add(tableModel);
		}

		return liquibaseModel;
	}

	private List<TableModel> associateTablesWithSchema(List<TableModel> tables, Database targetDatabase) {

		List<TableModel> processed = new ArrayList<>(tables.size());

		for (TableModel currentModel : tables) {

			if (currentModel.schema() == null || currentModel.schema().isEmpty()) {
				TableModel newModel = new TableModel(targetDatabase.getDefaultSchema().getCatalogName(), currentModel.name(),
						currentModel.columns(), currentModel.keyColumns());
				processed.add(newModel);
			} else {
				processed.add(currentModel);
			}
		}

		return processed;
	}

	private static AddColumnChange addColumns(TableDiff table) {

		AddColumnChange addColumnChange = new AddColumnChange();
		addColumnChange.setSchemaName(table.table().schema());
		addColumnChange.setTableName(table.table().name());

		for (ColumnModel column : table.columnsToAdd()) {
			AddColumnConfig addColumn = createAddColumnChange(column);
			addColumnChange.addColumn(addColumn);
		}
		return addColumnChange;
	}

	private static AddColumnConfig createAddColumnChange(ColumnModel column) {

		AddColumnConfig config = new AddColumnConfig();
		config.setName(column.name());
		config.setType(column.type());

		if (column.identityColumn()) {
			config.setAutoIncrement(true);
		}

		return config;
	}

	private static DropColumnChange dropColumns(TableDiff table, Collection<ColumnModel> deletedColumns) {

		DropColumnChange dropColumnChange = new DropColumnChange();
		dropColumnChange.setSchemaName(table.table().schema());
		dropColumnChange.setTableName(table.table().name());

		List<ColumnConfig> dropColumns = new ArrayList<>();

		for (ColumnModel column : deletedColumns) {
			ColumnConfig config = new ColumnConfig();
			config.setName(column.name());
			dropColumns.add(config);
		}

		dropColumnChange.setColumns(dropColumns);
		return dropColumnChange;
	}

	private static CreateTableChange changeTable(TableModel table) {

		CreateTableChange change = new CreateTableChange();
		change.setSchemaName(table.schema());
		change.setTableName(table.name());

		for (ColumnModel column : table.columns()) {
			ColumnConfig columnConfig = new ColumnConfig();
			columnConfig.setName(column.name());
			columnConfig.setType(column.type());

			if (column.identityColumn()) {
				columnConfig.setAutoIncrement(true);
				ConstraintsConfig constraints = new ConstraintsConfig();
				constraints.setPrimaryKey(true);
				columnConfig.setConstraints(constraints);
			}
			change.addColumn(columnConfig);
		}

		return change;
	}

	private static DropTableChange dropTable(TableModel table) {

		DropTableChange change = new DropTableChange();
		change.setSchemaName(table.schema());
		change.setTableName(table.name());
		change.setCascadeConstraints(true);

		return change;
	}
}
