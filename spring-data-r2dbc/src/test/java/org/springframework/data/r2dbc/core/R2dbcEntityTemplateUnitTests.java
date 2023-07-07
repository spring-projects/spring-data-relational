/*
 * Copyright 2020-2023 the original author or authors.
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

import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.event.AfterConvertCallback;
import org.springframework.data.r2dbc.mapping.event.AfterSaveCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.r2dbc.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.InsertOnlyProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link R2dbcEntityTemplate}.
 *
 * @author Mark Paluch
 * @author Jose Luis Leon
 * @author Robert Heim
 * @author Jens Schauder
 */
public class R2dbcEntityTemplateUnitTests {

    private org.springframework.r2dbc.core.DatabaseClient client;
    private R2dbcEntityTemplate entityTemplate;
    private StatementRecorder recorder;

    @BeforeEach
    void before() {

        recorder = StatementRecorder.newInstance();
        client = DatabaseClient.builder().connectionFactory(recorder)
                .bindMarkers(PostgresDialect.INSTANCE.getBindMarkersFactory()).build();
        entityTemplate = new R2dbcEntityTemplate(client, PostgresDialect.INSTANCE);
    }

    @Test
        // gh-220
    void shouldCountBy() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

        recorder.addStubbing(s -> s.startsWith("SELECT"), result);

        entityTemplate.count(Query.query(Criteria.where("name").is("Walter")), Person.class) //
                .as(StepVerifier::create) //
                .expectNext(1L) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

        assertThat(statement.getSql()).isEqualTo("SELECT COUNT(person.id) FROM person WHERE person.THE_NAME = $1");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-469
    void shouldProjectExistsResult() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().row(MockRow.builder().identified(0, Object.class, null).build()).build();

        recorder.addStubbing(s -> s.startsWith("SELECT"), result);

        entityTemplate.select(Person.class) //
                .as(Integer.class) //
                .matching(Query.empty().columns("MAX(age)")) //
                .all() //
                .as(StepVerifier::create) //
                .verifyComplete();
    }

    @Test
        // gh-1310
    void shouldProjectExistsResultWithoutId() {

        MockResult result = MockResult.builder().row(MockRow.builder().identified(0, Object.class, null).build()).build();

        recorder.addStubbing(s -> s.startsWith("SELECT 1"), result);

        entityTemplate.select(WithoutId.class).exists() //
                .as(StepVerifier::create) //
                .expectNext(true).verifyComplete();
    }

    @Test
        // gh-1310
    void shouldProjectCountResultWithoutId() {

        MockResult result = MockResult.builder().row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

        recorder.addStubbing(s -> s.startsWith("SELECT COUNT(1)"), result);

        entityTemplate.select(WithoutId.class).count() //
                .as(StepVerifier::create) //
                .expectNext(1L).verifyComplete();
    }

    @Test
        // gh-469
    void shouldExistsByCriteria() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

        recorder.addStubbing(s -> s.startsWith("SELECT"), result);

        entityTemplate.exists(Query.query(Criteria.where("name").is("Walter")), Person.class) //
                .as(StepVerifier::create) //
                .expectNext(true) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

        assertThat(statement.getSql()).isEqualTo("SELECT person.id FROM person WHERE person.THE_NAME = $1 LIMIT 1");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-220
    void shouldSelectByCriteria() {

        recorder.addStubbing(s -> s.startsWith("SELECT"), Collections.emptyList());

        entityTemplate.select(Query.query(Criteria.where("name").is("Walter")).sort(Sort.by("name")), Person.class) //
                .as(StepVerifier::create) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

        assertThat(statement.getSql())
                .isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 ORDER BY person.THE_NAME ASC");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-215
    void selectShouldInvokeCallback() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
                .columnMetadata(MockColumnMetadata.builder().name("THE_NAME").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().row(MockRow.builder().identified("id", Object.class, "Walter")
                .identified("THE_NAME", Object.class, "some-name").metadata(metadata).build()).build();

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

    @Test
        // gh-220
    void shouldSelectOne() {

        recorder.addStubbing(s -> s.startsWith("SELECT"), Collections.emptyList());

        entityTemplate.selectOne(Query.query(Criteria.where("name").is("Walter")).sort(Sort.by("name")), Person.class) //
                .as(StepVerifier::create) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

        assertThat(statement.getSql())
                .isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 ORDER BY person.THE_NAME ASC LIMIT 2");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-220, gh-758
    void shouldSelectOneDoNotOverrideExistingLimit() {

        recorder.addStubbing(s -> s.startsWith("SELECT"), Collections.emptyList());

        entityTemplate
                .selectOne(Query.query(Criteria.where("name").is("Walter")).sort(Sort.by("name")).limit(1), Person.class) //
                .as(StepVerifier::create) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

        assertThat(statement.getSql())
                .isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 ORDER BY person.THE_NAME ASC LIMIT 1");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-220
    void shouldUpdateByQuery() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

        entityTemplate
                .update(Query.query(Criteria.where("name").is("Walter")), Update.update("name", "Heisenberg"), Person.class) //
                .as(StepVerifier::create) //
                .expectNext(1L) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

        assertThat(statement.getSql()).isEqualTo("UPDATE person SET THE_NAME = $1 WHERE person.THE_NAME = $2");
        assertThat(statement.getBindings()).hasSize(2).containsEntry(0, Parameter.from("Heisenberg")).containsEntry(1,
                Parameter.from("Walter"));
    }

    @Test
        // gh-220
    void shouldDeleteByQuery() {

        MockRowMetadata metadata = MockRowMetadata.builder()
                .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()).build();
        MockResult result = MockResult.builder().rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("DELETE"), result);

        entityTemplate.delete(Query.query(Criteria.where("name").is("Walter")), Person.class) //
                .as(StepVerifier::create) //
                .expectNext(1L) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

        assertThat(statement.getSql()).isEqualTo("DELETE FROM person WHERE person.THE_NAME = $1");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-220
    void shouldDeleteEntity() {

        Person person = Person.empty() //
                .withId("Walter");
        recorder.addStubbing(s -> s.startsWith("DELETE"), Collections.emptyList());

        entityTemplate.delete(person) //
                .as(StepVerifier::create) //
                .expectNext(person).verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("DELETE"));

        assertThat(statement.getSql()).isEqualTo("DELETE FROM person WHERE person.id = $1");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("Walter"));
    }

    @Test
        // gh-365
    void shouldInsertVersioned() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        entityTemplate.insert(new VersionedPerson("id", 0, "bar")).as(StepVerifier::create) //
                .assertNext(actual -> {
                    assertThat(actual.version()).isEqualTo(1);
                }) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

        assertThat(statement.getSql()).isEqualTo("INSERT INTO versioned_person (id, version, name) VALUES ($1, $2, $3)");
        assertThat(statement.getBindings()).hasSize(3).containsEntry(0, Parameter.from("id")).containsEntry(1,
                Parameter.from(1L));
    }

    @Test
        // gh-557, gh-402
    void shouldSkipDefaultIdValueOnInsert() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        entityTemplate.insert(new PersonWithPrimitiveId(0, "bar")).as(StepVerifier::create) //
                .expectNextCount(1) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

        assertThat(statement.getSql()).isEqualTo("INSERT INTO person_with_primitive_id (name) VALUES ($1)");
        assertThat(statement.getBindings()).hasSize(1).containsEntry(0, Parameter.from("bar"));
    }

    @Test
        // gh-557, gh-402
    void shouldSkipDefaultIdValueOnVersionedInsert() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        entityTemplate.insert(new VersionedPersonWithPrimitiveId(0, 0, "bar")).as(StepVerifier::create) //
                .assertNext(actual -> {
                    assertThat(actual.version()).isEqualTo(1);
                }) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

        assertThat(statement.getSql())
                .isEqualTo("INSERT INTO versioned_person_with_primitive_id (version, name) VALUES ($1, $2)");
        assertThat(statement.getBindings()).hasSize(2).containsEntry(0, Parameter.from(1L)).containsEntry(1,
                Parameter.from("bar"));
    }

    @Test
        // gh-451
    void shouldInsertCorrectlyVersionedAndAudited() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        ObjectFactory<ReactiveIsNewAwareAuditingHandler> objectFactory = mock(ObjectFactory.class);
        when(objectFactory.getObject()).thenReturn(new ReactiveIsNewAwareAuditingHandler(
                PersistentEntities.of(entityTemplate.getConverter().getMappingContext())));

        entityTemplate
                .setEntityCallbacks(ReactiveEntityCallbacks.create(new ReactiveAuditingEntityCallback(objectFactory)));
        entityTemplate.insert(new WithAuditingAndOptimisticLocking(null, 0, "Walter", null, null)) //
                .as(StepVerifier::create) //
                .assertNext(actual -> {
                    assertThat(actual.version()).isEqualTo(1);
                    assertThat(actual.createdDate()).isNotNull();
                }) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

        assertThat(statement.getSql()).isEqualTo(
                "INSERT INTO with_auditing_and_optimistic_locking (version, name, created_date, last_modified_date) VALUES ($1, $2, $3, $4)");
    }

    @Test
        // gh-451
    void shouldUpdateCorrectlyVersionedAndAudited() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

        ObjectFactory<ReactiveIsNewAwareAuditingHandler> objectFactory = mock(ObjectFactory.class);
        when(objectFactory.getObject()).thenReturn(new ReactiveIsNewAwareAuditingHandler(
                PersistentEntities.of(entityTemplate.getConverter().getMappingContext())));

        entityTemplate
                .setEntityCallbacks(ReactiveEntityCallbacks.create(new ReactiveAuditingEntityCallback(objectFactory)));
        entityTemplate.update(new WithAuditingAndOptimisticLocking(null, 2, "Walter", null, null)) //
                .as(StepVerifier::create) //
                .assertNext(actual -> {
                    assertThat(actual.version()).isEqualTo(3);
                    assertThat(actual.createdDate()).isNull();
                    assertThat(actual.lastModifiedDate()).isNotNull();
                }) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

        assertThat(statement.getSql()).startsWith(
                "UPDATE with_auditing_and_optimistic_locking SET version = $1, name = $2, created_date = $3, last_modified_date = $4");
    }

    @Test
        // gh-215
    void insertShouldInvokeCallback() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        ValueCapturingBeforeConvertCallback beforeConvert = new ValueCapturingBeforeConvertCallback();
        ValueCapturingBeforeSaveCallback beforeSave = new ValueCapturingBeforeSaveCallback();
        ValueCapturingAfterSaveCallback afterSave = new ValueCapturingAfterSaveCallback();

        entityTemplate.setEntityCallbacks(ReactiveEntityCallbacks.create(beforeConvert, beforeSave, afterSave));
        entityTemplate.insert(Person.empty()).as(StepVerifier::create) //
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

    @Test
        // gh-365
    void shouldUpdateVersioned() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

        entityTemplate.update(new VersionedPerson("id", 1, "bar")).as(StepVerifier::create) //
                .assertNext(actual -> {
                    assertThat(actual.version()).isEqualTo(2);
                }) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

        assertThat(statement.getSql()).isEqualTo(
                "UPDATE versioned_person SET version = $1, name = $2 WHERE versioned_person.id = $3 AND (versioned_person.version = $4)");
        assertThat(statement.getBindings()).hasSize(4).containsEntry(0, Parameter.from(2L)).containsEntry(3,
                Parameter.from(1L));
    }

    @Test
        // gh-215
    void updateShouldInvokeCallback() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

        ValueCapturingBeforeConvertCallback beforeConvert = new ValueCapturingBeforeConvertCallback();
        ValueCapturingBeforeSaveCallback beforeSave = new ValueCapturingBeforeSaveCallback();
        ValueCapturingAfterSaveCallback afterSave = new ValueCapturingAfterSaveCallback();

        Person person = Person.empty() //
                .withId("the-id") //
                .withName("name") //
                .withDescription("description");

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

    @Test
        // gh-637
    void insertIncludesInsertOnlyColumns() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("INSERT"), result);

        entityTemplate.insert(new WithInsertOnly(null, "Alfred", "insert this")).as(StepVerifier::create) //
                .expectNextCount(1) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("INSERT"));

        assertThat(statement.getSql()).isEqualTo("INSERT INTO with_insert_only (name, insert_only) VALUES ($1, $2)");
        assertThat(statement.getBindings()).hasSize(2)
                .containsEntry(0, Parameter.from("Alfred"))
                .containsEntry(1, Parameter.from("insert this"));
    }

    @Test
        // gh-637
    void updateExcludesInsertOnlyColumns() {

        MockRowMetadata metadata = MockRowMetadata.builder().build();
        MockResult result = MockResult.builder().rowMetadata(metadata).rowsUpdated(1).build();

        recorder.addStubbing(s -> s.startsWith("UPDATE"), result);

        entityTemplate.update(new WithInsertOnly(23L, "Alfred", "don't update this")).as(StepVerifier::create) //
                .expectNextCount(1) //
                .verifyComplete();

        StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("UPDATE"));

        assertThat(statement.getSql()).isEqualTo("UPDATE with_insert_only SET name = $1 WHERE with_insert_only.id = $2");
        assertThat(statement.getBindings()).hasSize(2)
                .containsEntry(0, Parameter.from("Alfred"))
                .containsEntry(1, Parameter.from(23L));
    }

    record WithoutId(String name){
    }

    record Person (
        @Id
        String id,

        @Column("THE_NAME")
        String name,

        String description){

        public static Person empty() {
            return new Person(null, null, null);
        }

        public Person withId(String id) {
            return this.id == id ? this : new Person(id, this.name, this.description);
        }

        public Person withName(String name) {
            return this.name == name ? this : new Person(this.id, name, this.description);
        }

        public Person withDescription(String description) {
            return this.description == description ? this : new Person(this.id, this.name, description);
        }
    }

    record VersionedPerson(
        @Id
        String id,
        @Version
        long version,
         String name){

        public VersionedPerson withId(String id) {
            return this.id == id ? this : new VersionedPerson(id, this.version, this.name);
        }

        public VersionedPerson withVersion(long version) {
            return this.version == version ? this : new VersionedPerson(this.id, version, this.name);
        }

        public VersionedPerson withName(String name) {
            return this.name == name ? this : new VersionedPerson(this.id, this.version, name);
        }
    }

    record PersonWithPrimitiveId (
        @Id
        int id,
         String name
    ){
        public PersonWithPrimitiveId withId(int id) {
            return this.id == id ? this : new PersonWithPrimitiveId(id, this.name);
        }

        public PersonWithPrimitiveId withName(String name) {
            return this.name == name ? this : new PersonWithPrimitiveId(this.id, name);
        }
    }

    record VersionedPersonWithPrimitiveId (

        @Id
        int id,

        @Version
        long version,

        String name){

        public VersionedPersonWithPrimitiveId withId(int id) {
            return this.id == id ? this : new VersionedPersonWithPrimitiveId(id, this.version, this.name);
        }

        public VersionedPersonWithPrimitiveId withVersion(long version) {
            return this.version == version ? this : new VersionedPersonWithPrimitiveId(this.id, version, this.name);
        }

        public VersionedPersonWithPrimitiveId withName(String name) {
            return this.name == name ? this : new VersionedPersonWithPrimitiveId(this.id, this.version, name);
        }
    }

    record WithAuditingAndOptimisticLocking(

            @Id
            String id,

            @Version
            long version,

            String name,

            @CreatedDate
            LocalDateTime createdDate,
            @LastModifiedDate
            LocalDateTime lastModifiedDate) {
        public WithAuditingAndOptimisticLocking withId(String id) {
            return this.id == id ? this : new WithAuditingAndOptimisticLocking(id, version, name, createdDate, lastModifiedDate);
        }

        public WithAuditingAndOptimisticLocking withVersion(long version) {
            return this.version == version ? this : new WithAuditingAndOptimisticLocking(id, version, name, createdDate, lastModifiedDate);
        }

        public WithAuditingAndOptimisticLocking withName(String name) {
            return this.name == name ? this : new WithAuditingAndOptimisticLocking(id, version, name, createdDate, lastModifiedDate);
        }

        public WithAuditingAndOptimisticLocking withCreatedDate(LocalDateTime createdDate) {
            return this.createdDate == createdDate ? this : new WithAuditingAndOptimisticLocking(id, version, name, createdDate, lastModifiedDate);
        }

        public WithAuditingAndOptimisticLocking withLastModifiedDate(LocalDateTime lastModifiedDate) {
            return this.lastModifiedDate == lastModifiedDate ? this : new WithAuditingAndOptimisticLocking(id, version, name, createdDate, lastModifiedDate);
        }
    }

    record WithInsertOnly (
        @Id
        Long id,

        String name,

        @InsertOnlyProperty
        String insertOnly){
    }

    static class ValueCapturingEntityCallback<T> {

        private final List<T> values = new ArrayList<>(1);

        void capture(T value) {
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
            Person person = entity.withName("before-convert");
            return Mono.just(person);
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

            Person person = Person.empty() //
                    .withId("after-save") //
                    .withName(entity.name());

            return Mono.just(person);
        }
    }

    static class ValueCapturingAfterConvertCallback extends ValueCapturingEntityCallback<Person>
            implements AfterConvertCallback<Person> {

        @Override
        public Mono<Person> onAfterConvert(Person entity, SqlIdentifier table) {

            capture(entity);
            Person person = Person.empty() //
                    .withId("after-convert") //
                    .withName(entity.name());

            return Mono.just(person);
        }
    }
}
