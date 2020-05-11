/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import lombok.Value;
import lombok.With;
import reactor.test.StepVerifier;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;

/**
 * Unit tests for {@link R2dbcEntityTemplate}.
 *
 * @author Mark Paluch
 */
public class R2dbcEntityTemplateUnitTests {

	DatabaseClient client;
	R2dbcEntityTemplate entityTemplate;
	StatementRecorder recorder;

	@Before
	public void before() {

		recorder = StatementRecorder.newInstance();
		client = DatabaseClient.builder().connectionFactory(recorder)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();
		entityTemplate = new R2dbcEntityTemplate(client);
	}

	@Test // gh-220
	public void shouldCountBy() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.count(Query.query(Criteria.where("name").is("Walter")), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT COUNT(person.id) FROM person WHERE person.THE_NAME = $1");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldExistsByCriteria() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.exists(Query.query(Criteria.where("name").is("Walter")), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.id FROM person WHERE person.THE_NAME = $1 LIMIT 1");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldSelectByCriteria() {

		recorder.addStubbing(s -> s.startsWith("SELECT"), Collections.emptyList());

		entityTemplate.select(Query.query(Criteria.where("name").is("Walter")).sort(Sort.by("name")), Person.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql())
				.isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 ORDER BY THE_NAME ASC");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldSelectOne() {

		recorder.addStubbing(s -> s.startsWith("SELECT"), Collections.emptyList());

		entityTemplate.selectOne(Query.query(Criteria.where("name").is("Walter")).sort(Sort.by("name")), Person.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql())
				.isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 ORDER BY THE_NAME ASC LIMIT 2");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldUpdateByQuery() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

		entityTemplate
				.update(Query.query(Criteria.where("name").is("Walter")), Update.update("name", "Heisenberg"), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

		assertThat(statement.getSql()).isEqualTo("UPDATE person SET THE_NAME = $1 WHERE person.THE_NAME = $2");
		assertThat(statement.getBindings()).hasSize(2).containsEntry(0, SettableValue.from("Heisenberg")).containsEntry(1,
				SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldDeleteByQuery() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("DELETE"), result);

		entityTemplate.delete(Query.query(Criteria.where("name").is("Walter")), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

		assertThat(statement.getSql()).isEqualTo("DELETE FROM person WHERE person.THE_NAME = $1");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldDeleteEntity() {

		Person person = new Person();
		person.id = "Walter";
		recorder.addStubbing(s -> s.startsWith("DELETE"), Collections.emptyList());

		entityTemplate.delete(person) //
				.as(StepVerifier::create) //
				.expectNext(person).verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

		assertThat(statement.getSql()).isEqualTo("DELETE FROM person WHERE person.id = $1");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-365
	public void shouldInsertVersioned() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		entityTemplate.insert(new VersionedPerson("id", 0, "bar")).as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.getVersion()).isEqualTo(1);
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).isEqualTo("INSERT INTO versioned_person (id, version, name) VALUES ($1, $2, $3)");
		assertThat(statement.getBindings()).hasSize(3).containsEntry(0, SettableValue.from("id")).containsEntry(1,
				SettableValue.from(1L));
	}

	@Test // gh-365
	public void shouldUpdateVersioned() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

		entityTemplate.update(new VersionedPerson("id", 1, "bar")).as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.getVersion()).isEqualTo(2);
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

		assertThat(statement.getSql()).isEqualTo(
				"UPDATE versioned_person SET version = $1, name = $2 WHERE versioned_person.id = $3 AND (versioned_person.version = $4)");
		assertThat(statement.getBindings()).hasSize(4).containsEntry(0, SettableValue.from(2L)).containsEntry(3,
				SettableValue.from(1L));
	}

	static class Person {

		@Id String id;

		@Column("THE_NAME") String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	@Value
	@With
	static class VersionedPerson {

		@Id String id;

		@Version long version;

		String name;
	}
}
