/*
 * Copyright 2020-2024 the original author or authors.
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

import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;

/**
 * Unit test for {@link ReactiveInsertOperation}.
 *
 * @author Mark Paluch
 */
public class ReactiveInsertOperationUnitTests {

	private DatabaseClient client;
	private R2dbcEntityTemplate entityTemplate;
	private StatementRecorder recorder;

	@BeforeEach
	void before() {

		recorder = StatementRecorder.newInstance();
		client = DatabaseClient.builder().connectionFactory(recorder)
				.bindMarkers(PostgresDialect.INSTANCE.getBindMarkersFactory()).build();
		entityTemplate = new R2dbcEntityTemplate(client, new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));
	}

	@Test // gh-220
	void shouldInsert() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, 42).metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		Person person = new Person();
		person.setName("Walter");

		entityTemplate.insert(Person.class) //
				.using(person) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.id).isEqualTo("42");
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).isEqualTo("INSERT INTO person (THE_NAME) VALUES ($1)");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
	}

	@Test // gh-220
	void shouldUpdateInTable() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, 42).metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		Person person = new Person();
		person.setName("Walter");

		entityTemplate.insert(Person.class) //
				.into("the_table") //
				.using(person) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.id).isEqualTo("42");
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).isEqualTo("INSERT INTO the_table (THE_NAME) VALUES ($1)");
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

}
