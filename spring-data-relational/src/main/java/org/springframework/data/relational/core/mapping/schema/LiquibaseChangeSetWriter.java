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

package org.springframework.data.relational.core.mapping.schema;

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
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Predicates;
import org.springframework.util.Assert;

/**
 * Use this class to write Liquibase change sets.
 * <p>
 * First create a {@link Tables} instance passing in a {@link MappingContext} to have a model that represents the
 * Table(s)/Column(s) that the code expects to exist. And then optionally create a Liquibase database object that points
 * to an existing database if one desires to create a changeset that could be applied to that database. If a database
 * object is not used, then the change set created would be something that could be applied to an empty database to make
 * it match the state of the code. Prior to applying the changeset one should review and make adjustments appropriately.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class LiquibaseChangeSetWriter {

	public static final String DEFAULT_AUTHOR = "Spring Data Relational";
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	private SqlTypeMapping sqlTypeMapping = new DefaultSqlTypeMapping();

	private ChangeLogSerializer changeLogSerializer = new YamlChangeLogSerializer();

	private ChangeLogParser changeLogParser = new YamlChangeLogParser();

	/**
	 * Predicate to identify Liquibase system tables.
	 */
	private final Predicate<String> isLiquibaseTable = table -> table.toUpperCase(Locale.ROOT)
			.startsWith("DATABASECHANGELOG");

	/**
	 * Filter predicate to determine which persistent entities should be used for schema generation.
	 */
	public Predicate<RelationalPersistentEntity<?>> schemaFilter = Predicates.isTrue();

	/**
	 * Filter predicate used to determine whether an existing table should be removed. Defaults to {@code false} to keep
	 * existing tables.
	 */
	public Predicate<String> dropTableFilter = Predicates.isTrue();

	/**
	 * Filter predicate used to determine whether an existing column should be removed. Defaults to {@code false} to keep
	 * existing columns.
	 */
	public BiPredicate<String, String> dropColumnFilter = (table, column) -> false;

	/**
	 * Use this to generate a ChangeSet that can be used on an empty database.
	 *
	 * @param mappingContext source to determine persistent entities, must not be {@literal null}.
	 */
	public LiquibaseChangeSetWriter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null");

		this.mappingContext = mappingContext;
	}

	/**
	 * Configure SQL type mapping. Defaults to {@link DefaultSqlTypeMapping}.
	 *
	 * @param sqlTypeMapping must not be {@literal null}.
	 */
	public void setSqlTypeMapping(SqlTypeMapping sqlTypeMapping) {

		Assert.notNull(sqlTypeMapping, "SqlTypeMapping must not be null");

		this.sqlTypeMapping = sqlTypeMapping;
	}

	/**
	 * Set the {@link ChangeLogSerializer}.
	 *
	 * @param changeLogSerializer must not be {@literal null}.
	 */
	public void setChangeLogSerializer(ChangeLogSerializer changeLogSerializer) {

		Assert.notNull(changeLogSerializer, "ChangeLogSerializer must not be null");

		this.changeLogSerializer = changeLogSerializer;
	}

	/**
	 * Set the {@link ChangeLogParser}.
	 *
	 * @param changeLogParser must not be {@literal null}.
	 */
	public void setChangeLogParser(ChangeLogParser changeLogParser) {

		Assert.notNull(changeLogParser, "ChangeLogParser must not be null");

		this.changeLogParser = changeLogParser;
	}

	/**
	 * Set the filter predicate to identify for which tables to create schema definitions. Existing tables for excluded
	 * entities will show up in {@link #setDropTableFilter(Predicate)}. Returning {@code true} includes the entity;
	 * {@code false} excludes the entity from schema creation.
	 *
	 * @param schemaFilter must not be {@literal null}.
	 */
	public void setSchemaFilter(Predicate<RelationalPersistentEntity<?>> schemaFilter) {

		Assert.notNull(schemaFilter, "Schema filter must not be null");

		this.schemaFilter = schemaFilter;
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
	 * Write a Liquibase changeset containing all tables as initial changeset.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file). The
	 *          resource must resolve to a valid {@link Resource#getFile()}.
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource) throws IOException {
		writeChangeSet(changeLogResource, getChangeSetId(), DEFAULT_AUTHOR);
	}

	/**
	 * Write a Liquibase changeset using a {@link Database} to identify the differences between mapped entities and the
	 * existing database.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file). The
	 *          resource must resolve to a valid {@link Resource#getFile()}.
	 * @param database database to identify the differences.
	 * @throws LiquibaseException
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource, Database database) throws IOException, LiquibaseException {
		writeChangeSet(changeLogResource, getChangeSetId(), DEFAULT_AUTHOR, database);
	}

	/**
	 * Write a Liquibase changeset containing all tables as initial changeset.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file).
	 * @param changeSetId unique value to identify the changeset.
	 * @param changeSetAuthor author information to be written to changeset file.
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource, String changeSetId, String changeSetAuthor)
			throws IOException {

		DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogResource.getFile());
		ChangeSet changeSet = createChangeSet(changeSetId, changeSetAuthor, databaseChangeLog);

		writeChangeSet(databaseChangeLog, changeSet, changeLogResource.getFile());
	}

	/**
	 * Write a Liquibase changeset using a {@link Database} to identify the differences between mapped entities and the
	 * existing database.
	 *
	 * @param changeLogResource resource that changeset will be written to (or append to an existing ChangeSet file).
	 * @param changeSetId unique value to identify the changeset.
	 * @param changeSetAuthor author information to be written to changeset file.
	 * @param database database to identify the differences.
	 * @throws LiquibaseException
	 * @throws IOException
	 */
	public void writeChangeSet(Resource changeLogResource, String changeSetId, String changeSetAuthor, Database database)
			throws LiquibaseException, IOException {

		DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogResource.getFile());
		ChangeSet changeSet = createChangeSet(changeSetId, changeSetAuthor, database, databaseChangeLog);

		writeChangeSet(databaseChangeLog, changeSet, changeLogResource.getFile());
	}

	protected ChangeSet createChangeSet(String changeSetId, String changeSetAuthor, DatabaseChangeLog databaseChangeLog) {
		return createChangeSet(changeSetId, changeSetAuthor, createInitialDifference(), databaseChangeLog);
	}

	protected ChangeSet createChangeSet(String changeSetId, String changeSetAuthor, Database database,
			DatabaseChangeLog databaseChangeLog) throws LiquibaseException {
		return createChangeSet(changeSetId, changeSetAuthor, createSchemaDifference(database), databaseChangeLog);
	}

	private ChangeSet createChangeSet(String changeSetId, String changeSetAuthor, SchemaDiff difference,
			DatabaseChangeLog databaseChangeLog) {

		ChangeSet changeSet = new ChangeSet(changeSetId, changeSetAuthor, false, false, "", "", "", databaseChangeLog);

		generateTableAdditionsDeletions(changeSet, difference);
		generateTableModifications(changeSet, difference);
		return changeSet;
	}

	private SchemaDiff createInitialDifference() {

		Tables mappedEntities = Tables.from(mappingContext.getPersistentEntities().stream().filter(schemaFilter),
				sqlTypeMapping, null);
		return SchemaDiff.diff(mappedEntities, Tables.empty());
	}

	private SchemaDiff createSchemaDifference(Database database) throws LiquibaseException {

		Tables existingTables = getLiquibaseModel(database);
		Tables mappedEntities = Tables.from(mappingContext.getPersistentEntities().stream().filter(schemaFilter),
				sqlTypeMapping, database.getDefaultCatalogName());

		return SchemaDiff.diff(mappedEntities, existingTables);
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

		for (Table table : difference.tableAdditions()) {
			CreateTableChange newTable = changeTable(table);
			changeSet.addChange(newTable);
		}

		for (Table table : difference.tableDeletions()) {
			// Do not delete/drop table if it is an external application table
			if (dropTableFilter.test(table.name())) {
				changeSet.addChange(dropTable(table));
			}
		}
	}

	private void generateTableModifications(ChangeSet changeSet, SchemaDiff difference) {

		for (TableDiff table : difference.tableDiffs()) {

			if (!table.columnsToAdd().isEmpty()) {
				changeSet.addChange(addColumns(table));
			}

			List<Column> deletedColumns = getColumnsToDrop(table);

			if (deletedColumns.size() > 0) {
				changeSet.addChange(dropColumns(table, deletedColumns));
			}
		}
	}

	private List<Column> getColumnsToDrop(TableDiff table) {

		List<Column> deletedColumns = new ArrayList<>();
		for (Column column : table.columnsToDrop()) {

			if (dropColumnFilter.test(table.table().name(), column.name())) {
				deletedColumns.add(column);
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

	private Tables getLiquibaseModel(Database targetDatabase) throws LiquibaseException {

		CatalogAndSchema[] schemas = new CatalogAndSchema[] { targetDatabase.getDefaultSchema() };
		SnapshotControl snapshotControl = new SnapshotControl(targetDatabase);

		DatabaseSnapshot snapshot = SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, targetDatabase,
				snapshotControl);
		Set<liquibase.structure.core.Table> tables = snapshot.get(liquibase.structure.core.Table.class);
		List<Table> existingTables = new ArrayList<>(tables.size());

		for (liquibase.structure.core.Table table : tables) {

			// Exclude internal Liquibase tables from comparison
			if (isLiquibaseTable.test(table.getName())) {
				continue;
			}

			Table tableModel = new Table(table.getSchema().getCatalogName(), table.getName());

			List<liquibase.structure.core.Column> columns = table.getColumns();

			for (liquibase.structure.core.Column column : columns) {

				String type = column.getType().toString();
				boolean nullable = column.isNullable();
				Column columnModel = new Column(column.getName(), type, nullable, false);

				tableModel.columns().add(columnModel);
			}

			existingTables.add(tableModel);
		}

		return new Tables(existingTables);
	}

	private static String getChangeSetId() {
		return Long.toString(System.currentTimeMillis());
	}

	private static AddColumnChange addColumns(TableDiff table) {

		AddColumnChange addColumnChange = new AddColumnChange();
		addColumnChange.setSchemaName(table.table().schema());
		addColumnChange.setTableName(table.table().name());

		for (Column column : table.columnsToAdd()) {
			AddColumnConfig addColumn = createAddColumnChange(column);
			addColumnChange.addColumn(addColumn);
		}
		return addColumnChange;
	}

	private static AddColumnConfig createAddColumnChange(Column column) {

		AddColumnConfig config = new AddColumnConfig();
		config.setName(column.name());
		config.setType(column.type());

		if (column.identity()) {
			config.setAutoIncrement(true);
		}

		return config;
	}

	private static DropColumnChange dropColumns(TableDiff table, Collection<Column> deletedColumns) {

		DropColumnChange dropColumnChange = new DropColumnChange();
		dropColumnChange.setSchemaName(table.table().schema());
		dropColumnChange.setTableName(table.table().name());

		List<ColumnConfig> dropColumns = new ArrayList<>();

		for (Column column : deletedColumns) {
			ColumnConfig config = new ColumnConfig();
			config.setName(column.name());
			dropColumns.add(config);
		}

		dropColumnChange.setColumns(dropColumns);
		return dropColumnChange;
	}

	private static CreateTableChange changeTable(Table table) {

		CreateTableChange change = new CreateTableChange();
		change.setSchemaName(table.schema());
		change.setTableName(table.name());

		for (Column column : table.columns()) {
			ColumnConfig columnConfig = new ColumnConfig();
			columnConfig.setName(column.name());
			columnConfig.setType(column.type());

			if (column.identity()) {
				columnConfig.setAutoIncrement(true);
				ConstraintsConfig constraints = new ConstraintsConfig();
				constraints.setPrimaryKey(true);
				columnConfig.setConstraints(constraints);
			}
			change.addColumn(columnConfig);
		}

		return change;
	}

	private static DropTableChange dropTable(Table table) {

		DropTableChange change = new DropTableChange();
		change.setSchemaName(table.schema());
		change.setTableName(table.name());
		change.setCascadeConstraints(true);

		return change;
	}
}
