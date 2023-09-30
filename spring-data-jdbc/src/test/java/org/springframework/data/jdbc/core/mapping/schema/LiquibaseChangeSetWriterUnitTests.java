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
package org.springframework.data.jdbc.core.mapping.schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

import liquibase.change.Change;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.schema.LiquibaseChangeSetWriter.ChangeSetMetadata;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for {@link LiquibaseChangeSetWriter}.
 *
 * @author Mark Paluch
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

		Optional<Change> tableWithFk = changeSet.getChanges().stream().filter(change -> {
			return change instanceof CreateTableChange && ((CreateTableChange) change).getTableName()
					.equals("table_with_fk_field");
		}).findFirst();
		assertThat(tableWithFk.isPresent()).isEqualTo(true);

		List<ColumnConfig> columns = ((CreateTableChange) tableWithFk.get()).getColumns();
		assertThat(columns).extracting(ColumnConfig::getName).containsExactly("id", "tables_id");
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
		@MappedCollection
		Set<OtherTable> tables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class DifferentTables {
		@Id int id;
		@MappedCollection(idColumn = "tables_id")
		Set<TableWithFkField> tables;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class TableWithFkField {
		@Id int id;
		int tablesId;
	}

}
