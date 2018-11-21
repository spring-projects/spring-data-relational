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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;

/**
 * Integration tests for {@link DatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
public class PostgresDatabaseClientIntegrationTests extends AbstractDatabaseClientIntegrationTests {

	@ClassRule public static final ExternalDatabase database = PostgresTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return PostgresTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return PostgresTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return PostgresTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	protected String getInsertIntoLegosetStatement() {
		return PostgresTestSupport.INSERT_INTO_LEGOSET;
	}

	@Ignore("Adding RETURNING * lets Postgres report 0 affected rows.")
	@Override
	public void insert() {}

	@Ignore("Adding RETURNING * lets Postgres report 0 affected rows.")
	@Override
	public void insertTypedObject() {}
}
