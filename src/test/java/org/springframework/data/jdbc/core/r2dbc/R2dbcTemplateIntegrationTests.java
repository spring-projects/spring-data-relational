/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.r2dbc;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * @author Mark Paluch
 */
public class R2dbcTemplateIntegrationTests {

	private DataSource dataSource;

	private ConnectionFactory connectionFactory;

	private R2dbcTemplate template;

	private JdbcTemplate jdbc;

	@Before
	public void before() throws Exception {

		connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder().host("localhost")
				.database("postgres").username("postgres").password("").build());

		template = new R2dbcTemplate(connectionFactory);

		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUser("postgres");
		dataSource.setPassword("");
		dataSource.setDatabaseName("postgres");
		dataSource.setServerName("localhost");
		dataSource.setPortNumber(5432);

		String tableToCreate = "CREATE TABLE IF NOT EXISTS legoset (\n"
				+ "    id          integer CONSTRAINT id PRIMARY KEY,\n"
				+ "    name        varchar(255) NOT NULL,\n"
				+ "    manual      integer NULL\n" + ");";

		jdbc = new JdbcTemplate(dataSource);
		jdbc.execute(tableToCreate);
		jdbc.execute("DELETE FROM legoset");
	}

	@Test
	public void updateShouldInsertRecord() {

		Mono<Integer> insert = template
				.update("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', NULL)");

		insert.as(StepVerifier::create).expectNext(1).verifyComplete();

		List<Map<String, Object>> maps = jdbc.queryForList("SELECT * FROM legoset");
		assertThat(maps).hasSize(1);
	}

	@Test
	public void updateWithArgumentsShouldInsertRecord() {

		Mono<Integer> insert = template.update("INSERT INTO legoset (id, name) VALUES ($1, $2)", 42055,
				"SCHAUFELRADBAGGER");

		insert.as(StepVerifier::create).expectNext(1).verifyComplete();

		List<Map<String, Object>> maps = jdbc.queryForList("SELECT * FROM legoset");
		assertThat(maps).hasSize(1);
	}
}
