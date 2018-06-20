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
package org.springframework.data.jdbc.testing;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.data.jdbc.testing.ExternalDatabase.ProvidedDatabase;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base class for R2DBC integration tests.
 *
 * @author Mark Paluch
 */
public abstract class R2dbcIntegrationTestSupport {

	/**
	 * Local test database at {@code postgres:@localhost:5432/postgres}.
	 */
	@ClassRule public static final ExternalDatabase database = ProvidedDatabase.builder().hostname("localhost").port(5432)
			.database("postgres").username("postgres").password("").build();

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	protected static ConnectionFactory createConnectionFactory() {
		return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder().host(database.getHostname())
				.database(database.getDatabase()).username(database.getUsername()).password(database.getPassword()).build());
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	protected static DataSource createDataSource() {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setDatabaseName(database.getDatabase());
		dataSource.setServerName(database.getHostname());
		dataSource.setPortNumber(database.getPort());
		return dataSource;
	}

	/**
	 * Creates a new {@link JdbcTemplate} for a {@link DataSource}.
	 */
	protected JdbcTemplate createJdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
}
