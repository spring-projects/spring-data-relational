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

import liquibase.change.AddColumnConfig;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.DropColumnChange;
import liquibase.change.core.DropForeignKeyConstraintChange;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.H2Database;
import liquibase.database.jvm.JdbcConnection;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.ClassRelativeResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.schema.LiquibaseChangeSetWriter.ChangeSetMetadata;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.util.Predicates;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * Integration tests for {@link LiquibaseChangeSetWriter}.
 *
 * @author Mark Paluch
 * @author Evgenii Koba
 */
class LiquibaseChangeSetWriterIntegrationTests {

	@Test // GH-1430
	void shouldRemoveUnusedTable() {

		withEmbeddedDatabase("unused-table.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(new RelationalMappingContext());
			writer.setDropTableFilter(Predicates.isTrue());

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(1);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(DropTableChange.class);

			DropTableChange drop = (DropTableChange) changeSet.getChanges().get(0);
			assertThat(drop.getTableName()).isEqualToIgnoringCase("DELETE_ME");
		});
	}

	@Test // GH-1430
	void shouldNotDropTablesByDefault() {

		withEmbeddedDatabase("unused-table.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(new RelationalMappingContext());

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).isEmpty();
		});
	}

	@Test // GH-1430
	void shouldAddColumnToTable() {

		withEmbeddedDatabase("person-with-id-and-name.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(Person.class));

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(1);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(AddColumnChange.class);

			AddColumnChange addColumns = (AddColumnChange) changeSet.getChanges().get(0);
			assertThat(addColumns.getTableName()).isEqualToIgnoringCase("PERSON");
			assertThat(addColumns.getColumns()).hasSize(1);

			AddColumnConfig addColumn = addColumns.getColumns().get(0);
			assertThat(addColumn.getName()).isEqualTo("last_name");
			assertThat(addColumn.getType()).isEqualTo("VARCHAR(255 BYTE)");
		});
	}

	@Test // GH-1430
	void shouldRemoveColumnFromTable() {

		withEmbeddedDatabase("person-with-id-and-name.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(DifferentPerson.class));
			writer.setDropColumnFilter((s, s2) -> true);

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(2);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(AddColumnChange.class);

			AddColumnChange addColumns = (AddColumnChange) changeSet.getChanges().get(0);
			assertThat(addColumns.getTableName()).isEqualToIgnoringCase("PERSON");
			assertThat(addColumns.getColumns()).hasSize(2);
			assertThat(addColumns.getColumns()).extracting(AddColumnConfig::getName).containsExactly("my_id", "hello");

			DropColumnChange dropColumns = (DropColumnChange) changeSet.getChanges().get(1);
			assertThat(dropColumns.getTableName()).isEqualToIgnoringCase("PERSON");
			assertThat(dropColumns.getColumns()).hasSize(2);
			assertThat(dropColumns.getColumns()).extracting(ColumnConfig::getName).map(String::toUpperCase).contains("ID",
					"FIRST_NAME");
		});
	}

	@Test // GH-1430
	void doesNotRemoveColumnsByDefault() {

		withEmbeddedDatabase("person-with-id-and-name.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(DifferentPerson.class));

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(1);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(AddColumnChange.class);
		});
	}

	@Test // GH-1430
	void shouldCreateNewChangeLog(@TempDir File tempDir) {

		withEmbeddedDatabase("person-with-id-and-name.sql", c -> {

			File changelogYml = new File(tempDir, "changelog.yml");
			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(DifferentPerson.class));
			writer.writeChangeSet(new FileSystemResource(changelogYml));

			assertThat(tempDir).isDirectoryContaining(it -> it.getName().equalsIgnoreCase("changelog.yml"));

			assertThat(changelogYml).content().contains("author: Spring Data Relational").contains("name: hello");
		});
	}

	@Test // GH-1430
	void shouldAppendToChangeLog(@TempDir File tempDir) {

		withEmbeddedDatabase("person-with-id-and-name.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			File changelogYml = new File(tempDir, "changelog.yml");
			try (InputStream is = getClass().getResourceAsStream("changelog.yml")) {
				Files.copy(is, changelogYml.toPath());
			}

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(DifferentPerson.class));
			writer.writeChangeSet(new FileSystemResource(new File(tempDir, "changelog.yml")));

			assertThat(changelogYml).content().contains("author: Someone").contains("author: Spring Data Relational")
					.contains("name: hello");
		});
	}

	@Test // GH-1599
	void dropAndCreateTableWithRightOrderOfFkChanges() {

		withEmbeddedDatabase("drop-and-create-table-with-fk.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(GroupOfPersons.class));
			writer.setDropTableFilter(Predicates.isTrue());

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(4);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(DropForeignKeyConstraintChange.class);
			assertThat(changeSet.getChanges().get(3)).isInstanceOf(AddForeignKeyConstraintChange.class);

			DropForeignKeyConstraintChange dropForeignKey = (DropForeignKeyConstraintChange) changeSet.getChanges().get(0);
			assertThat(dropForeignKey.getConstraintName()).isEqualToIgnoringCase("fk_to_drop");
			assertThat(dropForeignKey.getBaseTableName()).isEqualToIgnoringCase("table_to_drop");

			AddForeignKeyConstraintChange addForeignKey = (AddForeignKeyConstraintChange) changeSet.getChanges().get(3);
			assertThat(addForeignKey.getBaseTableName()).isEqualToIgnoringCase("person");
			assertThat(addForeignKey.getBaseColumnNames()).isEqualToIgnoringCase("group_id");
			assertThat(addForeignKey.getReferencedTableName()).isEqualToIgnoringCase("group_of_persons");
			assertThat(addForeignKey.getReferencedColumnNames()).isEqualToIgnoringCase("id");
		});
	}

	@Test // GH-1599
	void dropAndCreateFkInRightOrder() {

		withEmbeddedDatabase("drop-and-create-fk.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(GroupOfPersons.class));
			writer.setDropColumnFilter((s, s2) -> true);

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(3);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(DropForeignKeyConstraintChange.class);
			assertThat(changeSet.getChanges().get(2)).isInstanceOf(AddForeignKeyConstraintChange.class);

			DropForeignKeyConstraintChange dropForeignKey = (DropForeignKeyConstraintChange) changeSet.getChanges().get(0);
			assertThat(dropForeignKey.getConstraintName()).isEqualToIgnoringCase("fk_to_drop");
			assertThat(dropForeignKey.getBaseTableName()).isEqualToIgnoringCase("person");

			AddForeignKeyConstraintChange addForeignKey = (AddForeignKeyConstraintChange) changeSet.getChanges().get(2);
			assertThat(addForeignKey.getBaseTableName()).isEqualToIgnoringCase("person");
			assertThat(addForeignKey.getBaseColumnNames()).isEqualToIgnoringCase("group_id");
			assertThat(addForeignKey.getReferencedTableName()).isEqualToIgnoringCase("group_of_persons");
			assertThat(addForeignKey.getReferencedColumnNames()).isEqualToIgnoringCase("id");
		});
	}

	@Test // GH-1599
	void fieldForFkWillBeCreated() {

		withEmbeddedDatabase("create-fk-with-field.sql", c -> {

			H2Database h2Database = new H2Database();
			h2Database.setConnection(new JdbcConnection(c));

			LiquibaseChangeSetWriter writer = new LiquibaseChangeSetWriter(contextOf(GroupOfPersons.class));

			ChangeSet changeSet = writer.createChangeSet(ChangeSetMetadata.create(), h2Database, new DatabaseChangeLog());

			assertThat(changeSet.getChanges()).hasSize(2);
			assertThat(changeSet.getChanges().get(0)).isInstanceOf(AddColumnChange.class);
			assertThat(changeSet.getChanges().get(1)).isInstanceOf(AddForeignKeyConstraintChange.class);

			AddColumnChange addColumn = (AddColumnChange) changeSet.getChanges().get(0);
			assertThat(addColumn.getTableName()).isEqualToIgnoringCase("person");
			assertThat(addColumn.getColumns()).hasSize(1);
			assertThat(addColumn.getColumns()).extracting(AddColumnConfig::getName).containsExactly("group_id");

			AddForeignKeyConstraintChange addForeignKey = (AddForeignKeyConstraintChange) changeSet.getChanges().get(1);
			assertThat(addForeignKey.getBaseTableName()).isEqualToIgnoringCase("person");
			assertThat(addForeignKey.getBaseColumnNames()).isEqualToIgnoringCase("group_id");
			assertThat(addForeignKey.getReferencedTableName()).isEqualToIgnoringCase("group_of_persons");
			assertThat(addForeignKey.getReferencedColumnNames()).isEqualToIgnoringCase("id");
		});
	}

	RelationalMappingContext contextOf(Class<?>... classes) {

		RelationalMappingContext context = new RelationalMappingContext();
		context.setInitialEntitySet(Set.of(classes));
		context.afterPropertiesSet();
		return context;
	}

	void withEmbeddedDatabase(String script, ThrowingConsumer<Connection> c) {

		EmbeddedDatabase embeddedDatabase = new EmbeddedDatabaseBuilder(new ClassRelativeResourceLoader(getClass())) //
				.generateUniqueName(true) //
				.setType(EmbeddedDatabaseType.H2) //
				.setScriptEncoding("UTF-8") //
				.ignoreFailedDrops(true) //
				.addScript(script) //
				.build();

		try {

			try (Connection connection = embeddedDatabase.getConnection()) {
				c.accept(connection);
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			embeddedDatabase.shutdown();
		}
	}

	@Table
	static class Person {
		@Id int id;
		String firstName;
		String lastName;
	}

	@Table("person")
	static class DifferentPerson {
		@Id int my_id;
		String hello;
	}

	@Table
	static class GroupOfPersons {
		@Id int id;
		@MappedCollection(idColumn = "group_id") Set<Person> persons;
	}

}
