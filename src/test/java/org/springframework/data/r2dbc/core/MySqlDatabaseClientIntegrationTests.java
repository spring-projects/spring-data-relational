/*
 * Copyright 2019-2020 the original author or authors.
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
import lombok.Data;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.MySqlTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link DatabaseClient} against MySQL.
 *
 * @author Mark Paluch
 */
public class MySqlDatabaseClientIntegrationTests extends AbstractDatabaseClientIntegrationTests {

	@ClassRule public static final ExternalDatabase database = MySqlTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return MySqlTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return MySqlTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return MySqlTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Test // gh-166
	public void considersBuiltInConverters() {

		ConnectionFactory connectionFactory = createConnectionFactory();
		JdbcTemplate jdbc = createJdbcTemplate(createDataSource());

		try {
			jdbc.execute("DROP TABLE boolean_mapping");
		} catch (DataAccessException e) {}
		jdbc.execute("CREATE TABLE boolean_mapping (id int, flag1 TINYINT, flag2 TINYINT)");

		BooleanMapping mapping = new BooleanMapping();
		mapping.setId(42);
		mapping.setFlag1(true);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(BooleanMapping.class).using(mapping).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		databaseClient.select().from(BooleanMapping.class).fetch().first() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.isFlag1()).isTrue()) //
				.verifyComplete();
	}

	@Ignore("https://github.com/mirromutth/r2dbc-mysql/issues/62")
	@Test
	@Override
	public void insertTypedObjectWithBinary() {}

	@Table("boolean_mapping")
	@Data
	static class BooleanMapping {

		int id;
		boolean flag1;
		boolean flag2;

	}

}
