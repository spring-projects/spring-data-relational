package org.springframework.data.r2dbc.testing;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Utility class for testing against Microsoft SQL Server.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 * @author Jens Schauder
 */
public class SqlServerTestSupport {
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
			+ "    flag      	 bit NULL\n," //
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
	 * Returns a locally provided database at {@code sqlserver:@localhost:1433/master}.
	 */
	public static ExternalDatabase database() {
		return local();
	}

	/**
	 * Returns a locally provided database at {@code sqlserver:@localhost:1433/master}.
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(1433) //
				.database("master") //
				.username("sa") //
				.password("A_Str0ng_Required_Password") //
				.build();
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
