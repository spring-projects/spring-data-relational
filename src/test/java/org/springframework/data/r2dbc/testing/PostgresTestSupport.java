package org.springframework.data.r2dbc.testing;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 */
public class PostgresTestSupport {

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
	 * Returns a locally provided database at {@code postgres:@localhost:5432/postgres}.
	 *
	 * @return
	 */
	public static ExternalDatabase database() {
		return local();
	}

	/**
	 * Returns a locally provided database at {@code postgres:@localhost:5432/postgres}.
	 *
	 * @return
	 */
	private static ExternalDatabase local() {
		return ProvidedDatabase.builder().hostname("localhost").port(5432).database("postgres").username("postgres")
				.password("").build();
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {
		return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder().host(database.getHostname())
				.database(database.getDatabase()).username(database.getUsername()).password(database.getPassword()).build());
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
