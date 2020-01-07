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
package org.springframework.data.r2dbc.testing;

import dev.miku.r2dbc.mysql.MySqlConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;

import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;

import org.testcontainers.containers.MySQLContainer;

import com.github.jasync.r2dbc.mysql.MysqlConnectionFactoryProvider;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Utility class for testing against MySQL.
 *
 * @author Mark Paluch
 */
public class MySqlTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    cert        varbinary(255) NULL\n" //
			+ ") ENGINE=InnoDB;";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          integer AUTO_INCREMENT PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ") ENGINE=InnoDB;";

	/**
	 * Returns a database either hosted locally at {@code postgres:@localhost:5432/postgres} or running inside Docker.
	 *
	 * @return information about the database. Guaranteed to be not {@literal null}.
	 */
	public static ExternalDatabase database() {

		if (Boolean.getBoolean("spring.data.r2dbc.test.preferLocalDatabase")) {

			return getFirstWorkingDatabase( //
					MySqlTestSupport::local, //
					MySqlTestSupport::testContainer //
			);
		} else {

			return getFirstWorkingDatabase( //
					MySqlTestSupport::testContainer, //
					MySqlTestSupport::local //
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
	 * Returns a locally provided database at {@code postgres:@localhost:5432/postgres}.
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(3306) //
				.database("mysql") //
				.username("root") //
				.password("my-secret-pw") //
				.jdbcUrl("jdbc:mysql://localhost:3306/mysql") //
				.build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {
				MySQLContainer container = new MySQLContainer();
				container.start();

				testContainerDatabase = ProvidedDatabase.builder(container) //
						.username("root") //
						.build();
			} catch (IllegalStateException ise) {
				// docker not available.
				testContainerDatabase = ExternalDatabase.unavailable();
			}
		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new Jasync MySQL {@link ConnectionFactory} configured from the {@link ExternalDatabase}.
	 */
	public static ConnectionFactory createJasyncConnectionFactory(ExternalDatabase database) {

		ConnectionFactoryOptions options = ConnectionUtils.createOptions("mysql", database);
		return new MysqlConnectionFactoryProvider().create(options);
	}

	/**
	 * Creates a new R2DBC MySQL {@link ConnectionFactory} configured from the {@link ExternalDatabase}.
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {

		ConnectionFactoryOptions options = ConnectionUtils.createOptions("mysql", database);
		return new MySqlConnectionFactoryProvider().create(options);
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		MysqlDataSource dataSource = new MysqlDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setURL(database.getJdbcUrl() + "?useSSL=false");

		return dataSource;
	}
}
