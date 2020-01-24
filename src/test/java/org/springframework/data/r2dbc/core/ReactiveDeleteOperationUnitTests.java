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
import static org.springframework.data.r2dbc.query.Criteria.*;
import static org.springframework.data.r2dbc.query.Query.*;

import io.r2dbc.spi.test.MockResult;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;

/**
 * Unit test for {@link ReactiveDeleteOperation}.
 *
 * @author Mark Paluch
 */
public class ReactiveDeleteOperationUnitTests {

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
	public void shouldDelete() {

		MockResult result = MockResult.builder().rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("DELETE"), result);

		entityTemplate.delete(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

		assertThat(statement.getSql()).isEqualTo("DELETE FROM person WHERE person.THE_NAME = $1");
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, SettableValue.from("Walter"));
	}

	@Test // gh-220
	public void shouldDeleteInTable() {

		MockResult result = MockResult.builder().rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("DELETE"), result);

		entityTemplate.delete(Person.class) //
				.from("other_table") //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

		assertThat(statement.getSql()).isEqualTo("DELETE FROM other_table WHERE other_table.THE_NAME = $1");
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
