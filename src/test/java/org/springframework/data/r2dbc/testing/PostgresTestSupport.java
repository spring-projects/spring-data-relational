package org.springframework.data.r2dbc.testing;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import java.util.function.Supplier;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class PostgresTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          serial CONSTRAINT id PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String INSERT_INTO_LEGOSET = "INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)";

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

	private static ExternalDatabase getFirstWorkingDatabase(Supplier<ExternalDatabase> first,
			Supplier<ExternalDatabase> second) {

		ExternalDatabase database = first.get();
		if (database.checkValidity()) {
			return database;
		} else {
			return second.get();
		}
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
				.password("").build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {
				PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer();
				postgreSQLContainer.start();

				testContainerDatabase = ProvidedDatabase.builder() //
						.hostname("localhost") //
						.port(postgreSQLContainer.getFirstMappedPort()) //
						.database(postgreSQLContainer.getDatabaseName()) //
						.username(postgreSQLContainer.getUsername()) //
						.password(postgreSQLContainer.getPassword()).build();

			} catch (IllegalStateException ise) {
				// docker is not available.
				testContainerDatabase = new ExternalDatabase.NoSuchDatabase();
			}

		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {

		return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder() //
				.host(database.getHostname()) //
				.database(database.getDatabase()) //
				.port(database.getPort()) //
				.username(database.getUsername()) //
				.password(database.getPassword()) //
				.build());
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setDatabaseName(database.getDatabase());
		dataSource.setServerName(database.getHostname());
		dataSource.setPortNumber(database.getPort());

		return dataSource;
	}

}
