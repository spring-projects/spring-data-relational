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
import lombok.ToString;
import lombok.Value;
import lombok.With;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.event.AfterConvertCallback;
import org.springframework.data.r2dbc.mapping.event.AfterSaveCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.CollectionUtils;

/**
 * Unit tests for {@link R2dbcEntityTemplate}.
 *
 * @author Mark Paluch
 */
public class R2dbcEntityTemplateUnitTests {

	org.springframework.r2dbc.core.DatabaseClient client;
	R2dbcEntityTemplate entityTemplate;
	StatementRecorder recorder;

	@Before
	public void before() {

		recorder = StatementRecorder.newInstance();
		client = DatabaseClient.builder().connectionFactory(recorder)
				.bindMarkers(PostgresDialect.INSTANCE.getBindMarkersFactory()).build();
		entityTemplate = new R2dbcEntityTemplate(client, PostgresDialect.INSTANCE);
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
	}

	@Test // gh-215
	public void selectShouldInvokeCallback() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.columnMetadata(MockColumnMetadata.builder().name("THE_NAME").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata).row(MockRow.builder()
				.identified("id", Object.class, "Walter").identified("THE_NAME", Object.class, "some-name").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		ValueCapturingAfterConvertCallback callback = new ValueCapturingAfterConvertCallback();

		entityTemplate.setEntityCallbacks(ReactiveEntityCallbacks.create(callback));

		entityTemplate.select(Query.empty(), Person.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.id).isEqualTo("after-convert");
					assertThat(actual.name).isEqualTo("some-name");
				}).verifyComplete();

		assertThat(callback.getValues()).hasSize(1);
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(2).containsEntry(0, Parameter.from("Heisenberg")).containsEntry(1,
				Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
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
		assertThat(statement.getBindings()).hasSize(3).containsEntry(0, Parameter.from("id")).containsEntry(1,
				Parameter.from(1L));
	}

	@Test // gh-215
	public void insertShouldInvokeCallback() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("INSERT"), result);

		ValueCapturingBeforeConvertCallback beforeConvert = new ValueCapturingBeforeConvertCallback();
		ValueCapturingBeforeSaveCallback beforeSave = new ValueCapturingBeforeSaveCallback();
		ValueCapturingAfterSaveCallback afterSave = new ValueCapturingAfterSaveCallback();

		entityTemplate.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvert, beforeSave, afterSave));
		entityTemplate.insert(new Person()).as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.id).isEqualTo("after-save");
					assertThat(actual.name).isEqualTo("before-convert");
					assertThat(actual.description).isNull();
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

		assertThat(statement.getSql()).isEqualTo("INSERT INTO person (THE_NAME, description) VALUES ($1, $2)");
		assertThat(statement.getBindings()).hasSize(2).containsEntry(0, Parameter.from("before-convert")).containsEntry(1,
				Parameter.from("before-save"));
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
		assertThat(statement.getBindings()).hasSize(4).containsEntry(0, Parameter.from(2L)).containsEntry(3,
				Parameter.from(1L));
	}

	@Test // gh-215
	public void updateShouldInvokeCallback() {

		MockRowMetadata metadata = MockRowMetadata.builder().build();
		MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

		recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

		ValueCapturingBeforeConvertCallback beforeConvert = new ValueCapturingBeforeConvertCallback();
		ValueCapturingBeforeSaveCallback beforeSave = new ValueCapturingBeforeSaveCallback();
		ValueCapturingAfterSaveCallback afterSave = new ValueCapturingAfterSaveCallback();

		Person person = new Person();
		person.id = "the-id";
		person.name = "name";
		person.description = "description";

		entityTemplate.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvert, beforeSave, afterSave));
		entityTemplate.update(person).as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.id).isEqualTo("after-save");
					assertThat(actual.name).isEqualTo("before-convert");
					assertThat(actual.description).isNull();
				}) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

		assertThat(statement.getSql()).isEqualTo("UPDATE person SET THE_NAME = $1, description = $2 WHERE person.id = $3");
		assertThat(statement.getBindings()).hasSize(3).containsEntry(0, Parameter.from("before-convert")).containsEntry(1,
				Parameter.from("before-save"));
	}

	@ToString
	static class Person {

		@Id String id;

		@Column("THE_NAME") String name;

		String description;

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

	static class ValueCapturingEntityCallback<T> {

		private final List<T> values = new ArrayList<>(1);

		protected void capture(T value) {
			values.add(value);
		}

		public List<T> getValues() {
			return values;
		}

		@Nullable
		public T getValue() {
			return CollectionUtils.lastElement(values);
		}
	}

	static class ValueCapturingBeforeConvertCallback extends ValueCapturingEntityCallback<Person>
			implements BeforeConvertCallback<Person> {

		@Override
		public Mono<Person> onBeforeConvert(Person entity, SqlIdentifier table) {

			capture(entity);
			entity.name = "before-convert";
			return Mono.just(entity);
		}
	}

	static class ValueCapturingBeforeSaveCallback extends ValueCapturingEntityCallback<Person>
			implements BeforeSaveCallback<Person> {

		@Override
		public Mono<Person> onBeforeSave(Person entity, OutboundRow outboundRow, SqlIdentifier table) {

			capture(entity);
			outboundRow.put(SqlIdentifier.unquoted("description"), Parameter.from("before-save"));
			return Mono.just(entity);
		}
	}

	static class ValueCapturingAfterSaveCallback extends ValueCapturingEntityCallback<Person>
			implements AfterSaveCallback<Person> {

		@Override
		public Mono<Person> onAfterSave(Person entity, OutboundRow outboundRow, SqlIdentifier table) {

			capture(entity);

			Person person = new Person();
			person.id = "after-save";
			person.name = entity.name;

			return Mono.just(person);
		}
	}

	static class ValueCapturingAfterConvertCallback extends ValueCapturingEntityCallback<Person>
			implements AfterConvertCallback<Person> {

		@Override
		public Mono<Person> onAfterConvert(Person entity, SqlIdentifier table) {

			capture(entity);
			Person person = new Person();
			person.id = "after-convert";
			person.name = entity.name;

			return Mono.just(person);
		}
	}
}
