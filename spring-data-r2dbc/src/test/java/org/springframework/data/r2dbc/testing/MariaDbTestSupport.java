/*
 * Copyright 2019-2024 the original author or authors.
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
import io.r2dbc.spi.ConnectionFactoryOptions;

import java.sql.SQLException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.r2dbc.MariadbConnectionFactoryProvider;
import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Utility class for testing against MariaDB.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class MariaDbTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static final String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    cert        varbinary(255) NULL\n" //
			+ ") ENGINE=InnoDB;";

	public static final String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          integer AUTO_INCREMENT PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    flag        boolean NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ") ENGINE=InnoDB;";

	public static final String CREATE_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES = "CREATE TABLE `LegoSet` (\n" //
			+ "    `Id`          integer AUTO_INCREMENT PRIMARY KEY,\n" //
			+ "    `Name`        varchar(255) NOT NULL,\n" //
			+ "    `Manual`      integer NULL\n" //
			+ ") ENGINE=InnoDB;";

	public static final String DROP_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES = "DROP TABLE `LegoSet`";

	/**
	 * Returns a database either hosted locally at {@code localhost:3306/mysql} or running inside Docker.
	 *
	 * @return information about the database. Guaranteed to be not {@literal null}.
	 */
	public static ExternalDatabase database() {

		if (Boolean.getBoolean("spring.data.r2dbc.test.preferLocalDatabase")) {

			return getFirstWorkingDatabase( //
					MariaDbTestSupport::local, //
					MariaDbTestSupport::testContainer //
			);
		} else {

			return getFirstWorkingDatabase( //
					MariaDbTestSupport::testContainer, //
					MariaDbTestSupport::local //
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
	 * Returns a locally provided database .
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(3306) //
				.database("mysql") //
				.username("root") //
				.password("my-secret-pw") //
				.jdbcUrl("jdbc:mariadb://localhost:3306/mysql") //
				.build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {

				String osArch = System.getProperty("os.arch");

				DockerImageName armImageName = DockerImageName.parse("arm64v8/mariadb:10.3")
						.asCompatibleSubstituteFor("mariadb");

				DockerImageName mariadb = DockerImageName.parse("mariadb").withTag("10.3.6");
				var container = new MariaDBContainer<>("aarch64".equals(osArch) ? armImageName : mariadb);

				container.start();

				testContainerDatabase = ProvidedDatabase.builder(container) //
						.username("root") //
						.database(container.getDatabaseName()) //
						.build();
			} catch (IllegalStateException ise) {
				// docker not available.
				testContainerDatabase = ExternalDatabase.unavailable();
			}
		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new R2DBC MariaDB {@link ConnectionFactory} configured from the {@link ExternalDatabase}.
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {

		ConnectionFactoryOptions options = ConnectionUtils.createOptions("mariadb", database);
		return new MariadbConnectionFactoryProvider().create(options);
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		try {
			MariaDbDataSource dataSource = new MariaDbDataSource();

			dataSource.setUser(database.getUsername());
			dataSource.setUrl(String.format("jdbc:mariadb://%s:%d/%s?", database.getHostname(), database.getPort(),
					database.getDatabase()));
			dataSource.setPassword(database.getPassword());

			return dataSource;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
