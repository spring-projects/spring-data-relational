/*
 * Copyright 2026-present the original author or authors.
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

import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRowMetadata;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;

/**
 * Unit tests for {@link ReactiveUpsertOperation}.
 *
 * @author Christoph Strobl
 */
public class ReactiveUpsertOperationUnitTests {

	private DatabaseClient client;
	private R2dbcEntityTemplate entityTemplate;
	private StatementRecorder recorder;

	@BeforeEach
	void before() {

		recorder = StatementRecorder.newInstance();
		client = DatabaseClient.builder().connectionFactory(recorder)
				.bindMarkers(PostgresDialect.INSTANCE.getBindMarkersFactory()).build();
		entityTemplate = new R2dbcEntityTemplate(client, new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));
		((R2dbcMappingContext) entityTemplate.getDataAccessStrategy().getConverter().getMappingContext())
				.setForceQuote(false);
	}

	@Test // GH-493
	void shouldUpsert() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		Person person = new Person();
		person.id = 42L;
		person.setName("Walter");

		entityTemplate.upsert(Person.class) //
				.one(person) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.id).isEqualTo(42L);
					assertThat(actual.getName()).isEqualTo("Walter");
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).startsWith("INSERT INTO person");
		assertThat(statement.getSql()).contains("ON CONFLICT");
		assertThat(statement.getSql()).contains("DO UPDATE SET");
		assertThat(statement.getBindings()).hasSize(2) //
				.containsEntry(0, Parameter.from(42L)) //
				.containsEntry(1, Parameter.from("Walter"));
	}

	@Test // GH-493
	void shouldUpsertInTable() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		Person person = new Person();
		person.id = 42L;
		person.setName("Walter");

		entityTemplate.upsert(Person.class) //
				.inTable("the_table") //
				.one(person) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.id).isEqualTo(42L);
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).startsWith("INSERT INTO the_table");
	}

	@Test // GH-493
	void shouldUpsertViaTemplateMethod() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		Person person = new Person();
		person.id = 42L;
		person.setName("Walter");

		entityTemplate.upsert(person) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.id).isEqualTo(42L);
					assertThat(actual.getName()).isEqualTo("Walter");
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).startsWith("INSERT INTO person");
		assertThat(statement.getSql()).contains("ON CONFLICT");
	}

	@Test // GH-493
	void upsertIncludesInsertOnlyColumns() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		entityTemplate.upsert(Person.class) //
				.one(new Person(42L, "Alfred", "insert this")) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).startsWith("INSERT INTO person");
		assertThat(statement.getSql()).contains("THE_NAME");
		assertThat(statement.getSql()).contains("insert_only");
	}

	static class Person {

		@Id Long id;

		@Column("THE_NAME") String name;

		@org.springframework.data.relational.core.mapping.InsertOnlyProperty
		String insertOnly;

		Person() {}

		Person(Long id, String name, String insertOnly) {
			this.id = id;
			this.name = name;
			this.insertOnly = insertOnly;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

}
