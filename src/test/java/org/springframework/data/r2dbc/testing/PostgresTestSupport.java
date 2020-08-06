package org.springframework.data.r2dbc.testing;

import io.r2dbc.spi.ConnectionFactory;

import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Bogdan Ilchyshyn
 */
public class PostgresTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    cert        bytea NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          serial CONSTRAINT id PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	/**
	 * Returns a database either hosted locally at {@code postgres:@localhost:5432/postgres} or running inside Docker.
	 *
	 * @return information about the database. Guaranteed to be not {@literal null}.
	 */
	public static ExternalDatabase database() {

		if (Boolean.getBoolean("spring.data.r2dbc.test.preferLocalDatabase")) {

			return getFirstWorkingDatabase( //
					PostgresTestSupport::local, //
					PostgresTestSupport::testContainer //
			);
		} else {

			return getFirstWorkingDatabase( //
					PostgresTestSupport::testContainer, //
					PostgresTestSupport::local //
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
				.port(5432) //
				.database("postgres") //
				.username("postgres") //
				.password("") //
				.jdbcUrl("jdbc:postgresql://localhost/postgres") //
				.build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {
				PostgreSQLContainer container = new PostgreSQLContainer(
						PostgreSQLContainer.IMAGE + ":" + PostgreSQLContainer.DEFAULT_TAG);
				container.start();

				testContainerDatabase = ProvidedDatabase.from(container);

			} catch (IllegalStateException ise) {
				// docker not available.
				testContainerDatabase = ExternalDatabase.unavailable();
			}

		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {
		return ConnectionUtils.getConnectionFactory("postgresql", database);
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setURL(database.getJdbcUrl());

		return dataSource;
	}
}
