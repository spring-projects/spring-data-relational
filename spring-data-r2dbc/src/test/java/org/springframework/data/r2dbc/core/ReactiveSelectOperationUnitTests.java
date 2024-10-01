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
import static org.springframework.data.relational.core.query.Criteria.*;
import static org.springframework.data.relational.core.query.Query.*;

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

/**
 * Unit test for {@link ReactiveSelectOperation}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
public class ReactiveSelectOperationUnitTests {

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

	@Test // GH-220
	void shouldSelectAll() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter")).limit(10).offset(20)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql())
				.isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 10 OFFSET 20");
	}

	@Test // GH-220
	void shouldSelectAs() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.as(PersonProjection.class) //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.THE_NAME FROM person WHERE person.THE_NAME = $1");
	}

	@Test // GH-220, GH-1690
	void shouldSelectAsWithColumnName() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.columnMetadata(MockColumnMetadata.builder().name("a_different_name").type(R2dbcType.VARCHAR).build()).build();
		MockResult result = MockResult.builder().row(MockRow.builder().identified("id", Object.class, "Walter")
				.identified("a_different_name", Object.class, "Werner").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.as(PersonProjectionWithColumnName.class) //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.getName()).isEqualTo("Werner")) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.id, person.a_different_name FROM person WHERE person.THE_NAME = $1");
	}

	@Test // GH-220
	void shouldSelectFromTable() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.from("the_table") //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT the_table.* FROM the_table WHERE the_table.THE_NAME = $1");
	}

	@Test // GH-220
	void shouldSelectFirst() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.first() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 1");
	}

	@Test // GH-220
	void shouldSelectOne() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.one() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 2");
	}

	@Test // GH-220
	void shouldSelectExists() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.exists() //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT 1 FROM person WHERE person.THE_NAME = $1 LIMIT 1");
	}

	@Test // GH-220
	void shouldSelectCount() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified(0, Long.class, 1L).metadata(metadata).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT COUNT(*) FROM person WHERE person.THE_NAME = $1");
	}

	@Test // GH-1652
	void shouldConsiderFetchSize() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
				.build();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified("id", Object.class, "Walter").metadata(metadata).build())
				.build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.withFetchSize(10) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getFetchSize()).isEqualTo(10);
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

	static class PersonProjectionWithColumnName {

		@Id String id;

		@Column("a_different_name") String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	interface PersonProjection {

		String getName();
	}
}
