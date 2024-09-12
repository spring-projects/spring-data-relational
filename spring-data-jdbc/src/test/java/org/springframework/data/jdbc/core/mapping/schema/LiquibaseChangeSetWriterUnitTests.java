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

import static org.assertj.core.api.Assertions.*;

import liquibase.change.Change;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.schema.LiquibaseChangeSetWriter.ChangeSetMetadata;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for {@link LiquibaseChangeSetWriter}.
 *
 * @author Mark Paluch
 * @author Evgenii Koba
 */
class LiquibaseChangeSetWriterUnitTests {

	@Test // GH-1480
	void newTableShouldCreateChangeSet() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(VariousTypes.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		CreateTableChange createTable = (CreateTableChange) changeSet.getChanges().get(0);

		assertThat(createTable.getColumns()).extracting(ColumnConfig::getName).containsSequence("id",
				"luke_i_am_your_father", "dark_side", "floater");
		assertThat(createTable.getColumns()).extracting(ColumnConfig::getType).containsSequence("BIGINT",
				"VARCHAR(255 BYTE)", "TINYINT", "FLOAT");

		ColumnConfig id = createTable.getColumns().get(0);
		assertThat(id.getConstraints().isNullable()).isFalse();
	}

	@Test // GH-1480
	void shouldApplySchemaFilter() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(VariousTypes.class);
		context.getRequiredPersistentEntity(OtherTable.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);
		writer.setSchemaFilter(it -> it.getName().contains("OtherTable"));

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		assertThat(changeSet.getChanges()).hasSize(1);
		CreateTableChange createTable = (CreateTableChange) changeSet.getChanges().get(0);

		assertThat(createTable.getTableName()).isEqualTo("other_table");
	}

	@Test // GH-1599
	void createForeignKeyWithNewTable() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(Tables.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		AddForeignKeyConstraintChange addForeignKey = (AddForeignKeyConstraintChange) changeSet.getChanges().get(2);

		assertThat(addForeignKey.getBaseTableName()).isEqualTo("other_table");
		assertThat(addForeignKey.getBaseColumnNames()).isEqualTo("tables");
		assertThat(addForeignKey.getReferencedTableName()).isEqualTo("tables");
		assertThat(addForeignKey.getReferencedColumnNames()).isEqualTo("id");

	}

	@Test // GH-1599
	void fieldForFkShouldNotBeCreatedTwice() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(DifferentTables.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		Optional<Change> tableWithFk = changeSet.getChanges().stream().filter(change -> change instanceof CreateTableChange createTableChange
				&& createTableChange.getTableName().equals("table_with_fk_field")).findFirst();
		assertThat(tableWithFk.isPresent()).isEqualTo(true);

		List<ColumnConfig> columns = ((CreateTableChange) tableWithFk.get()).getColumns();
		assertThat(columns).extracting(ColumnConfig::getName).containsExactly("id", "tables_id");
	}

	@Test // GH-1599
	void createForeignKeyForNestedEntities() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(ListOfMapOfNoIdTables.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		assertCreateTable(changeSet, "no_id_table", Tuple.tuple("field", "VARCHAR(255 BYTE)", null),
				Tuple.tuple("list_id", "INT", true), Tuple.tuple("list_of_map_of_no_id_tables_key", "INT", true),
				Tuple.tuple("map_of_no_id_tables_key", "VARCHAR(255 BYTE)", true));

		assertCreateTable(changeSet, "map_of_no_id_tables", Tuple.tuple("list_id", "INT", true),
				Tuple.tuple("list_of_map_of_no_id_tables_key", "INT", true));

		assertCreateTable(changeSet, "list_of_map_of_no_id_tables", Tuple.tuple("id", "INT", true));

		assertAddForeignKey(changeSet, "no_id_table", "list_id,list_of_map_of_no_id_tables_key", "map_of_no_id_tables",
				"list_id,list_of_map_of_no_id_tables_key");

		assertAddForeignKey(changeSet, "map_of_no_id_tables", "list_id", "list_of_map_of_no_id_tables", "id");
	}

	@Test // GH-1599
	void createForeignKeyForOneToOneWithMultipleChildren() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(OneToOneLevel1.class);

		LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(context);

		ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), new DatabaseChangeLog());

		assertCreateTable(changeSet, "other_table", Tuple.tuple("id", "BIGINT", true),
				Tuple.tuple("one_to_one_level1", "INT", null));

		assertCreateTable(changeSet, "one_to_one_level2", Tuple.tuple("one_to_one_level1", "INT", true));

		assertCreateTable(changeSet, "no_id_table", Tuple.tuple("field", "VARCHAR(255 BYTE)", null),
				Tuple.tuple("one_to_one_level2", "INT", true), Tuple.tuple("additional_one_to_one_level2", "INT", null));

		assertAddForeignKey(changeSet, "other_table", "one_to_one_level1", "one_to_one_level1", "id");

		assertAddForeignKey(changeSet, "one_to_one_level2", "one_to_one_level1", "one_to_one_level1", "id");

		assertAddForeignKey(changeSet, "no_id_table", "one_to_one_level2", "one_to_one_level2", "one_to_one_level1");

		assertAddForeignKey(changeSet, "no_id_table", "additional_one_to_one_level2", "one_to_one_level2",
				"one_to_one_level1");

	}


	void assertCreateTable(ChangeSet changeSet, String tableName, Tuple... columnTuples) {
		Optional<Change> createTableOptional = changeSet.getChanges().stream().filter(change -> change instanceof CreateTableChange createTableChange && createTableChange.getTableName().equals(tableName)).findFirst();
		assertThat(createTableOptional.isPresent()).isTrue();
		CreateTableChange createTable = (CreateTableChange) createTableOptional.get();
		assertThat(createTable.getColumns())
				.extracting(ColumnConfig::getName, ColumnConfig::getType, column -> column.getConstraints().isPrimaryKey())
				.containsExactly(columnTuples);
	}

	void assertAddForeignKey(ChangeSet changeSet, String baseTableName, String baseColumnNames,
			String referencedTableName, String referencedColumnNames) {
		Optional<Change> addFkOptional = changeSet.getChanges().stream().filter(change -> change instanceof AddForeignKeyConstraintChange addForeignKeyConstraintChange
				&& addForeignKeyConstraintChange.getBaseTableName().equals(baseTableName)
				&& addForeignKeyConstraintChange.getBaseColumnNames().equals(baseColumnNames)).findFirst();
		assertThat(addFkOptional.isPresent()).isTrue();
		AddForeignKeyConstraintChange addFk = (AddForeignKeyConstraintChange) addFkOptional.get();
		assertThat(addFk.getBaseTableName()).isEqualTo(baseTableName);
		assertThat(addFk.getBaseColumnNames()).isEqualTo(baseColumnNames);
		assertThat(addFk.getReferencedTableName()).isEqualTo(referencedTableName);
		assertThat(addFk.getReferencedColumnNames()).isEqualTo(referencedColumnNames);
	}

	@org.springframework.data.relational.core.mapping.Table
	static class VariousTypes {
		@Id long id;
		String lukeIAmYourFather;
		Boolean darkSide;
		Float floater;
		Double doubleClass;
		Integer integerClass;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class OtherTable {
		@Id long id;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class Tables {
		@Id int id;
		@MappedCollection Set<OtherTable> tables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class SetOfTables {
		@Id int id;
		@MappedCollection(idColumn = "set_id") Set<Tables> setOfTables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class DifferentTables {
		@Id int id;
		@MappedCollection(idColumn = "tables_id") Set<TableWithFkField> tables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class TableWithFkField {
		@Id int id;
		int tablesId;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class NoIdTable {
		String field;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class MapOfNoIdTables {
		@MappedCollection Map<String, NoIdTable> tables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class ListOfMapOfNoIdTables {
		@Id int id;
		@MappedCollection(idColumn = "list_id") List<MapOfNoIdTables> listOfTables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class OneToOneLevel1 {
		@Id int id;
		OneToOneLevel2 oneToOneLevel2;
		OtherTable otherTable;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class OneToOneLevel2 {
		NoIdTable table1;
		@Column("additional_one_to_one_level2") NoIdTable table2;
	}

}
