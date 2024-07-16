/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.jdbc.core.mapping.schema;

import liquibase.CatalogAndSchema;
import liquibase.change.AddColumnConfig;
import liquibase.change.ColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.CreateTableChange;
import liquibase.change.core.DropColumnChange;
import liquibase.change.core.DropForeignKeyConstraintChange;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeLogChild;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.ChangeLogParseException;
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
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.core.io.Resource;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Predicates;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Use this class to write Liquibase ChangeSets.
 * <p>
 * This writer uses {@link MappingContext} as input to determine mapped entities. Entities can be filtered through a
 * {@link #setSchemaFilter(Predicate) schema filter} to include/exclude entities. By default, all entities within the
 * mapping context are considered for computing the expected schema.
 * <p>
 * This writer operates in two modes:
 * <ul>
 * <li>Initial Schema Creation</li>
 * <li>Differential Schema Change Creation</li>
 * </ul>
 * The {@link #writeChangeSet(Resource) initial mode} allows creating the full schema without considering any existing
 * tables. The {@link #writeChangeSet(Resource, Database) differential schema mode} uses a {@link Database} object to
 * determine existing tables and columns. It creates in addition to table creations also changes to drop tables, drop
 * columns and add columns. By default, the {@link #setDropTableFilter(Predicate) DROP TABLE} and the
 * {@link #setDropColumnFilter(BiPredicate) DROP COLUMN} filters exclude all tables respective columns from being
 * dropped.
 * <p>
 * In differential schema mode, table and column names are compared using a case-insensitive comparator, see
 * {@link Collator#PRIMARY}.
 * <p>
 * The writer can be configured to use specific ChangeLogSerializers and ChangeLogParsers defaulting to YAML.
 *
 * @author Kurt Niemi
 * @author Mark Paluch
 * @author Evgenii Koba
 * @author Jens Schauder
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
	 * Comparator to compare table and column names.
	 */
	private final Comparator<String> nameComparator = createComparator();

	private static Comparator<String> createComparator() {

		Collator instance = Collator.getInstance(Locale.ROOT);
		instance.setStrength(Collator.PRIMARY);

		return instance::compare;
	}

	/**
	 * Filter predicate to determine which persistent entities should be used for schema generation.
	 */
	private Predicate<RelationalPersistentEntity<?>> schemaFilter = Predicates.isTrue();

	/**
	 * Filter predicate used to determine whether an existing table should be removed. Defaults to {@code false} to keep
	 * existing tables.
	 */
	private Predicate<String> dropTableFilter = Predicates.isFalse();

	/**
	 * Filter predicate used to determine whether an existing column should be removed. Defaults to {@code false} to keep
	 * existing columns.
	 */
	private BiPredicate<String, String> dropColumnFilter = (table, column) -> false;

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
	 * Set the filter predicate to identify for which entities to create schema definitions. Existing tables for excluded
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
	 * Write a Liquibase ChangeSet containing all tables as initial ChangeSet.
	 *
	 * @param changeLogResource resource that ChangeSet will be written to (or append to an existing ChangeSet file). The
	 *          resource must resolve to a valid {@link Resource#getFile()}.
	 * @throws IOException in case of I/O errors.
	 */
	public void writeChangeSet(Resource changeLogResource) throws IOException {
		writeChangeSet(changeLogResource, ChangeSetMetadata.create());
	}

	/**
	 * Write a Liquibase ChangeSet using a {@link Database} to identify the differences between mapped entities and the
	 * existing database.
	 *
	 * @param changeLogResource resource that ChangeSet will be written to (or append to an existing ChangeSet file). The
	 *          resource must resolve to a valid {@link Resource#getFile()}.
	 * @param database database to identify the differences.
	 * @throws LiquibaseException
	 * @throws IOException in case of I/O errors.
	 */
	public void writeChangeSet(Resource changeLogResource, Database database) throws IOException, LiquibaseException {
		writeChangeSet(changeLogResource, ChangeSetMetadata.create(), database);
	}

	/**
	 * Write a Liquibase ChangeSet containing all tables as initial ChangeSet.
	 *
	 * @param changeLogResource resource that ChangeSet will be written to (or append to an existing ChangeSet file).
	 * @param metadata the ChangeSet metadata.
	 * @throws IOException in case of I/O errors.
	 */
	public void writeChangeSet(Resource changeLogResource, ChangeSetMetadata metadata) throws IOException {

		DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogResource.getFile(), null);
		ChangeSet changeSet = createChangeSet(metadata, databaseChangeLog);

		writeChangeSet(databaseChangeLog, changeSet, changeLogResource.getFile());
	}

	/**
	 * Write a Liquibase ChangeSet using a {@link Database} to identify the differences between mapped entities and the
	 * existing database.
	 *
	 * @param changeLogResource resource that ChangeSet will be written to (or append to an existing ChangeSet file).
	 * @param metadata the ChangeSet metadata.
	 * @param database database to identify the differences.
	 * @throws LiquibaseException
	 * @throws IOException in case of I/O errors.
	 */
	public void writeChangeSet(Resource changeLogResource, ChangeSetMetadata metadata, Database database)
			throws LiquibaseException, IOException {

		DatabaseChangeLog databaseChangeLog = getDatabaseChangeLog(changeLogResource.getFile(), database);
		ChangeSet changeSet = createChangeSet(metadata, database, databaseChangeLog);

		writeChangeSet(databaseChangeLog, changeSet, changeLogResource.getFile());
	}

	/**
	 * Creates an initial ChangeSet.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param databaseChangeLog must not be {@literal null}.
	 * @return the initial ChangeSet.
	 */
	protected ChangeSet createChangeSet(ChangeSetMetadata metadata, DatabaseChangeLog databaseChangeLog) {
		return createChangeSet(metadata, initial(), databaseChangeLog);
	}

	/**
	 * Creates a diff ChangeSet by comparing {@link Database} with {@link MappingContext mapped entities}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param databaseChangeLog must not be {@literal null}.
	 * @return the diff ChangeSet.
	 */
	protected ChangeSet createChangeSet(ChangeSetMetadata metadata, Database database,
			DatabaseChangeLog databaseChangeLog) throws LiquibaseException {
		return createChangeSet(metadata, differenceOf(database), databaseChangeLog);
	}

	private ChangeSet createChangeSet(ChangeSetMetadata metadata, SchemaDiff difference,
			DatabaseChangeLog databaseChangeLog) {

		ChangeSet changeSet = new ChangeSet(metadata.getId(), metadata.getAuthor(), false, false, "", "", "",
				databaseChangeLog);

		generateTableAdditionsDeletions(changeSet, difference);
		generateTableModifications(changeSet, difference);
		return changeSet;
	}

	private SchemaDiff initial() {

		Stream<? extends RelationalPersistentEntity<?>> entities = mappingContext.getPersistentEntities().stream()
				.filter(schemaFilter);
		Tables mappedEntities = Tables.from(entities, sqlTypeMapping, null, mappingContext);
		return SchemaDiff.diff(mappedEntities, Tables.empty(), nameComparator);
	}

	private SchemaDiff differenceOf(Database database) throws LiquibaseException {

		Tables existingTables = getLiquibaseModel(database);
		Stream<? extends RelationalPersistentEntity<?>> entities = mappingContext.getPersistentEntities().stream()
				.filter(schemaFilter);
		Tables mappedEntities = Tables.from(entities, sqlTypeMapping, database.getDefaultSchemaName(), mappingContext);

		return SchemaDiff.diff(mappedEntities, existingTables, nameComparator);
	}

	private DatabaseChangeLog getDatabaseChangeLog(File changeLogFile, @Nullable Database database) throws IOException {

		ChangeLogParameters parameters = database != null ? new ChangeLogParameters(database) : new ChangeLogParameters();

		if (!changeLogFile.exists()) {
			DatabaseChangeLog databaseChangeLog = new DatabaseChangeLog(changeLogFile.getName());
			if (database != null) {
				databaseChangeLog.setChangeLogParameters(parameters);
			}
			return databaseChangeLog;
		}

		try {

			File parentDirectory = changeLogFile.getParentFile();
			if (parentDirectory == null) {
				parentDirectory = new File("./");
			}

			DirectoryResourceAccessor resourceAccessor = new DirectoryResourceAccessor(parentDirectory);
			return changeLogParser.parse(changeLogFile.getName(), parameters, resourceAccessor);
		} catch (ChangeLogParseException ex) {
			throw new IOException(ex);
		}
	}

	private void generateTableAdditionsDeletions(ChangeSet changeSet, SchemaDiff difference) {

		for (Table table : difference.tableDeletions()) {
			for (ForeignKey foreignKey : table.foreignKeys()) {
				DropForeignKeyConstraintChange dropForeignKey = dropForeignKey(foreignKey);
				changeSet.addChange(dropForeignKey);
			}
		}

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

		for (Table table : difference.tableAdditions()) {
			for (ForeignKey foreignKey : table.foreignKeys()) {
				AddForeignKeyConstraintChange addForeignKey = addForeignKey(foreignKey);
				changeSet.addChange(addForeignKey);
			}
		}
	}

	private void generateTableModifications(ChangeSet changeSet, SchemaDiff difference) {

		for (TableDiff table : difference.tableDiffs()) {

			for (ForeignKey foreignKey : table.fkToDrop()) {
				DropForeignKeyConstraintChange dropForeignKey = dropForeignKey(foreignKey);
				changeSet.addChange(dropForeignKey);
			}

			if (!table.columnsToAdd().isEmpty()) {
				changeSet.addChange(addColumns(table));
			}

			List<Column> deletedColumns = getColumnsToDrop(table);

			if (!deletedColumns.isEmpty()) {
				changeSet.addChange(dropColumns(table, deletedColumns));
			}

			for (ForeignKey foreignKey : table.fkToAdd()) {
				AddForeignKeyConstraintChange addForeignKey = addForeignKey(foreignKey);
				changeSet.addChange(addForeignKey);
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

			Table tableModel = new Table(table.getSchema().getName(), table.getName());

			List<liquibase.structure.core.Column> columns = table.getColumns();

			for (liquibase.structure.core.Column column : columns) {

				String type = column.getType().toString();
				boolean nullable = column.isNullable();
				Column columnModel = new Column(column.getName(), type, nullable, false);

				tableModel.columns().add(columnModel);
			}

			tableModel.foreignKeys().addAll(extractForeignKeys(table));

			existingTables.add(tableModel);
		}

		return new Tables(existingTables);
	}

	private static List<ForeignKey> extractForeignKeys(liquibase.structure.core.Table table) {

		return table.getOutgoingForeignKeys().stream().map(foreignKey -> {

			String tableName = foreignKey.getForeignKeyTable().getName();
			List<String> columnNames = foreignKey.getForeignKeyColumns().stream()
					.map(liquibase.structure.core.Column::getName).toList();

			String referencedTableName = foreignKey.getPrimaryKeyTable().getName();
			List<String> referencedColumnNames = foreignKey.getPrimaryKeyColumns().stream()
					.map(liquibase.structure.core.Column::getName).toList();

			return new ForeignKey(foreignKey.getName(), tableName, columnNames, referencedTableName, referencedColumnNames);
		}).collect(Collectors.toList());
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

			ConstraintsConfig constraints = new ConstraintsConfig();
			constraints.setNullable(column.nullable());

			if (column.identity()) {

				columnConfig.setAutoIncrement(true);
				constraints.setPrimaryKey(true);
			}

			columnConfig.setConstraints(constraints);
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

	private static AddForeignKeyConstraintChange addForeignKey(ForeignKey foreignKey) {

		AddForeignKeyConstraintChange change = new AddForeignKeyConstraintChange();
		change.setConstraintName(foreignKey.name());
		change.setBaseTableName(foreignKey.tableName());
		change.setBaseColumnNames(String.join(",", foreignKey.columnNames()));
		change.setReferencedTableName(foreignKey.referencedTableName());
		change.setReferencedColumnNames(String.join(",", foreignKey.referencedColumnNames()));

		return change;
	}

	private static DropForeignKeyConstraintChange dropForeignKey(ForeignKey foreignKey) {

		DropForeignKeyConstraintChange change = new DropForeignKeyConstraintChange();
		change.setConstraintName(foreignKey.name());
		change.setBaseTableName(foreignKey.tableName());

		return change;
	}

	/**
	 * Metadata for a ChangeSet.
	 */
	interface ChangeSetMetadata {

		/**
		 * Creates a new default {@link ChangeSetMetadata} using the {@link #DEFAULT_AUTHOR default author}.
		 *
		 * @return a new default {@link ChangeSetMetadata} using the {@link #DEFAULT_AUTHOR default author}.
		 */
		static ChangeSetMetadata create() {
			return ofAuthor(LiquibaseChangeSetWriter.DEFAULT_AUTHOR);
		}

		/**
		 * Creates a new default {@link ChangeSetMetadata} using a generated {@code identifier} and provided {@code author}.
		 *
		 * @return a new default {@link ChangeSetMetadata} using a generated {@code identifier} and provided {@code author}.
		 */
		static ChangeSetMetadata ofAuthor(String author) {
			return of(Long.toString(System.currentTimeMillis()), author);
		}

		/**
		 * Creates a new default {@link ChangeSetMetadata} using the provided {@code identifier} and {@code author}.
		 *
		 * @return a new default {@link ChangeSetMetadata} using the provided {@code identifier} and {@code author}.
		 */
		static ChangeSetMetadata of(String identifier, String author) {
			return new DefaultChangeSetMetadata(identifier, author);
		}

		/**
		 * @return the ChangeSet identifier.
		 */
		String getId();

		/**
		 * @return the ChangeSet author.
		 */
		String getAuthor();
	}

	private record DefaultChangeSetMetadata(String id, String author) implements ChangeSetMetadata {

		private DefaultChangeSetMetadata {

			Assert.hasText(id, "ChangeSet identifier must not be empty or null");
			Assert.hasText(author, "Author must not be empty or null");
		}

		@Override
		public String getId() {
			return id();
		}

		@Override
		public String getAuthor() {
			return author();
		}
	}
}
