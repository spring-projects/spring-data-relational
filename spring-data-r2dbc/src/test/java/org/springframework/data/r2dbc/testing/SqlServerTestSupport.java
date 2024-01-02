/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.testing;

import io.r2dbc.spi.ConnectionFactory;

import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;
import org.testcontainers.containers.MSSQLServerContainer;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Utility class for testing against Microsoft SQL Server.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 * @author Jens Schauder
 */
public class SqlServerTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    cert        varbinary(255) NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          integer IDENTITY(1,1) PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    flag        bit NULL\n," //
			+ "    extra       varchar(255),\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String INSERT_INTO_LEGOSET = "INSERT INTO legoset (id, name, manual) VALUES(@P0, @P1, @P3)";

	public static final String CREATE_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES = "CREATE TABLE LegoSet (\n" //
			+ "    Id          integer IDENTITY(1,1) PRIMARY KEY,\n" //
			+ "    Name        varchar(255) NOT NULL,\n" //
			+ "    Manual      integer NULL\n" //
			+ ");";

	public static final String DROP_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES = "DROP TABLE LegoSet";

	/**
	 * Returns a database either hosted locally at {@code jdbc:sqlserver://localhost:1433;database=master;} or running
	 * inside Docker.
	 */
	public static ExternalDatabase database() {

		// Disable SQL Server support as there's no M1 support yet.
		if (ConnectionUtils.AARCH64.equals(System.getProperty("os.arch"))) {
			return ExternalDatabase.unavailable();
		}

		if (Boolean.getBoolean("spring.data.r2dbc.test.preferLocalDatabase")) {

			return getFirstWorkingDatabase( //
					SqlServerTestSupport::local, //
					SqlServerTestSupport::testContainer //
			);
		} else {

			return getFirstWorkingDatabase( //
					SqlServerTestSupport::testContainer, //
					SqlServerTestSupport::local //
			);
		}
	}

	@SafeVarargs
	private static ExternalDatabase getFirstWorkingDatabase(Supplier<ExternalDatabase>... suppliers) {

		return Stream.of(suppliers).map(Supplier::get) //
				.filter(ExternalDatabase::checkValidity) //
				.findFirst() //
				.orElse(ExternalDatabase.unavailable());
	}

	/**
	 * Returns a locally provided database at {@code jdbc:sqlserver://localhost:1433;database=master}.
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(1433) //
				.database("master") //
				.username("sa") //
				.password("A_Str0ng_Required_Password") //
				.jdbcUrl("jdbc:sqlserver://localhost:1433;database=master;encrypt=false")
				.build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	@SuppressWarnings("rawtypes")
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {
				MSSQLServerContainer<?> container = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest") {
					@Override
					public String getDatabaseName() {
						return "master";
					}
				};
				container.withReuse(true);
				container.withUrlParam("encrypt", "false");
				container.start();

				testContainerDatabase = ProvidedDatabase.builder(container).database(container.getDatabaseName()).build();

			} catch (IllegalStateException ise) {
				// docker not available.
				testContainerDatabase = ExternalDatabase.unavailable();
			}

		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}.
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {
		return ConnectionUtils.getConnectionFactory("mssql", database);
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		SQLServerDataSource dataSource = new SQLServerDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setURL(database.getJdbcUrl());

		return dataSource;
	}
}
