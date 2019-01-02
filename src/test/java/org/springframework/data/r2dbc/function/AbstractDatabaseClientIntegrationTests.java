/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.function;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.Sort.Order.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Data;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
public abstract class AbstractDatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

		connectionFactory = createConnectionFactory();

		jdbc = createJdbcTemplate(createDataSource());

		try {
			jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}
		jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Returns the the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	/**
	 * Get a parameterized {@code INSERT INTO legoset} statement setting id, name, and manual values.
	 */
	protected abstract String getInsertIntoLegosetStatement();

	@Test // gh-2
	public void executeInsert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute().sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void shouldTranslateDuplicateKeyException() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		executeInsert();

		databaseClient.execute().sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectErrorSatisfies(exception -> assertThat(exception) //
						.isInstanceOf(DuplicateKeyException.class) //
						.hasMessageContaining("execute; SQL [INSERT INTO legoset")) //
				.verify();
	}

	@Test // gh-2
	public void executeSelect() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute().sql("SELECT id, name, manual FROM legoset") //
				.as(LegoSet.class) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test // gh-2
	public void insert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("legoset")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1).verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void insertWithoutResult() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("legoset")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void insertTypedObject() {

		LegoSet legoSet = new LegoSet();
		legoSet.setId(42055);
		legoSet.setName("SCHAUFELRADBAGGER");
		legoSet.setManual(12);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(LegoSet.class)//
				.using(legoSet) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void selectAsMap() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.fetch() //
				.all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test // gh-8
	public void selectExtracting() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("legoset") //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.map((r, md) -> r.get("id", Integer.class)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(42055) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectOrderByIdDesc() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectOrderPaged() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.page(PageRequest.of(1, 1)).fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42064) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectTypedLater() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("legoset") //
				.orderBy(Sort.by(desc("id"))) //
				.as(LegoSet.class) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	@Data
	@Table("legoset")
	static class LegoSet {

		int id;
		String name;
		Integer manual;
	}
}
