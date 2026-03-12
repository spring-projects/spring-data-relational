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

import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.InsertOnlyProperty;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Abstract base class for {@link R2dbcEntityTemplate} upsert integration tests across different databases.
 *
 * @author Christoph Strobl
 */
public abstract class AbstractR2dbcEntityTemplateUpsertIntegrationTests extends R2dbcIntegrationTestSupport {

	protected JdbcTemplate jdbc;
	protected DatabaseClient client;
	protected R2dbcEntityTemplate entityTemplate;

	@BeforeEach
	void setUp() {

		jdbc = createJdbcTemplate(createDataSource());
		ConnectionFactory connectionFactory = createConnectionFactory();
		client = DatabaseClient.create(connectionFactory);
		entityTemplate = createEntityTemplate(connectionFactory);

		try {
			jdbc.execute(getDropLegosetStatement());
		} catch (DataAccessException ignore) {}

		jdbc.execute(getCreateLegosetStatement());

		try {
			jdbc.execute(getDropWithInsertOnlyStatement());
		} catch (DataAccessException ignore) {}

		jdbc.execute(getCreateWithInsertOnlyStatement());
	}

	/**
	 * @return the {@link DataSource} for JDBC-based test setup.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * @return the {@link ConnectionFactory} for R2DBC operations.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Creates the {@link R2dbcEntityTemplate} for the given {@link ConnectionFactory}. Subclasses may override to
	 * customize, e.g., to disable identifier quoting via {@link R2dbcMappingContext#setForceQuote(boolean)}.
	 */
	protected R2dbcEntityTemplate createEntityTemplate(ConnectionFactory connectionFactory) {
		return new R2dbcEntityTemplate(connectionFactory);
	}

	/**
	 * @return the CREATE TABLE statement for {@code legoset} with explicit (non-generated) integer primary key, name and
	 *         manual columns.
	 */
	protected abstract String getCreateLegosetStatement();

	/**
	 * @return the CREATE TABLE statement for {@code with_insert_only} with explicit (non-generated) integer primary key
	 *         and an {@code insert_only} varchar column.
	 */
	protected abstract String getCreateWithInsertOnlyStatement();

	/**
	 * @return the DROP TABLE statement for {@code legoset}. Subclasses may override when the table identifier requires
	 *         explicit quoting (e.g. Oracle).
	 */
	protected String getDropLegosetStatement() {
		return "DROP TABLE legoset";
	}

	/**
	 * @return the DROP TABLE statement for {@code with_insert_only}. Subclasses may override when the table identifier
	 *         requires explicit quoting (e.g. Oracle).
	 */
	protected String getDropWithInsertOnlyStatement() {
		return "DROP TABLE with_insert_only";
	}

	@Test // GH-493
	void upsertInsertsWhenIdDoesNotExist() {

		LegoSet lego = new LegoSet(8888L, "star-wars", 10);

		entityTemplate.upsert(lego) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.id).isEqualTo(8888L)) //
				.verifyComplete();

		entityTemplate.select(LegoSet.class).matching(org.springframework.data.relational.core.query.Query.empty()).all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.id).isEqualTo(8888L);
					assertThat(actual.name).isEqualTo("star-wars");
				}) //
				.verifyComplete();
	}

	@Test // GH-493
	void upsertUpdatesWhenIdExists() {

		LegoSet first = new LegoSet(8888L, "first", 10);
		LegoSet second = new LegoSet(8888L, "second", 20);

		entityTemplate.upsert(first) //
				.then(entityTemplate.upsert(second)) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.name).isEqualTo("second")) //
				.verifyComplete();

		entityTemplate.select(LegoSet.class).matching(org.springframework.data.relational.core.query.Query.empty()).all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.id).isEqualTo(8888L);
					assertThat(actual.name).isEqualTo("second");
				}) //
				.verifyComplete();
	}

	@Test // GH-493
	void upsertAfterDeleteInsertsAgain() {

		LegoSet first = new LegoSet(8888L, "first", 10);
		LegoSet second = new LegoSet(8888L, "second", 20);

		entityTemplate.upsert(first) //
				.then(entityTemplate.delete(first)) //
				.then(entityTemplate.upsert(second)) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.name).isEqualTo("second")) //
				.verifyComplete();

		entityTemplate.select(LegoSet.class).matching(org.springframework.data.relational.core.query.Query.empty()).all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.name).isEqualTo("second")) //
				.verifyComplete();
	}

	@Test // GH-493
	void upsertNoOpWhenNonKeyColumnsAlreadyMatch() {

		LegoSet lego = new LegoSet(8888L, "millennium", 5);

		entityTemplate.upsert(lego) //
				.then(entityTemplate.upsert(lego)) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.name).isEqualTo("millennium")) //
				.verifyComplete();

		entityTemplate.select(LegoSet.class).matching(org.springframework.data.relational.core.query.Query.empty()).all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.name).isEqualTo("millennium");
				}) //
				.verifyComplete();
	}

	@Test // GH-493
	void upsertIncludesInsertOnlyColumnOnInsert() {

		WithInsertOnly entity = new WithInsertOnly(8888L, "initial");

		entityTemplate.upsert(entity) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.insertOnly).isEqualTo("initial")) //
				.verifyComplete();

		entityTemplate.select(WithInsertOnly.class).matching(org.springframework.data.relational.core.query.Query.empty())
				.all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.insertOnly).isEqualTo("initial")) //
				.verifyComplete();
	}

	@Test // GH-493
	void upsertViaFluentApiInTable() {

		LegoSet lego = new LegoSet(8888L, "test", 1);

		entityTemplate.upsert(LegoSet.class) //
				.one(lego) //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual.id).isEqualTo(8888L)) //
				.verifyComplete();
	}

	@Table("legoset")
	static class LegoSet {

		@Id Long id;
		String name;
		Integer manual;

		LegoSet() {}

		LegoSet(Long id, String name, Integer manual) {
			this.id = id;
			this.name = name;
			this.manual = manual;
		}
	}

	@Table("with_insert_only")
	static class WithInsertOnly {

		@Id Long id;

		@InsertOnlyProperty String insertOnly;

		WithInsertOnly() {}

		WithInsertOnly(Long id, String insertOnly) {
			this.id = id;
			this.insertOnly = insertOnly;
		}
	}
}
