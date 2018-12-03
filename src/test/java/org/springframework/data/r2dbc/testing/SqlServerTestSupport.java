package org.springframework.data.r2dbc.testing;

import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Utility class for testing against Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
public class SqlServerTestSupport {

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          integer IDENTITY(1,1) PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String INSERT_INTO_LEGOSET = "INSERT INTO legoset (id, name, manual) VALUES(@P0, @P1, @P3)";

	/**
	 * Returns a locally provided database at {@code sqlserver:@localhost:1433/master}.
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
		return ProvidedDatabase.builder().hostname("localhost").port(1433).database("master").username("sa")
				.password("A_Str0ng_Required_Password").build();
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {
		return new MssqlConnectionFactory(MssqlConnectionConfiguration.builder().host(database.getHostname()) //
				.database(database.getDatabase()) //
				.username(database.getUsername()) //
				.password(database.getPassword()) //
				.build());
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		SQLServerDataSource dataSource = new SQLServerDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setDatabaseName(database.getDatabase());
		dataSource.setServerName(database.getHostname());
		dataSource.setPortNumber(database.getPort());

		return dataSource;
	}
}
