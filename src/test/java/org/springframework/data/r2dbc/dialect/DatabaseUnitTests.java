package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;

import org.junit.Test;
import org.reactivestreams.Publisher;

/**
 * Unit tests for {@link Database}.
 *
 * @author Mark Paluch
 */
public class DatabaseUnitTests {

	@Test // gh-20
	public void shouldResolveDatabaseType() {

		PostgresqlConnectionFactory postgres = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
				.host("localhost").database("foo").username("bar").password("password").build());
		MssqlConnectionFactory mssql = new MssqlConnectionFactory(MssqlConnectionConfiguration.builder().host("localhost")
				.database("foo").username("bar").password("password").build());
		H2ConnectionFactory h2 = new H2ConnectionFactory(H2ConnectionConfiguration.builder().inMemory("mem").build());

		assertThat(Database.findDatabase(postgres)).contains(Database.POSTGRES);
		assertThat(Database.findDatabase(mssql)).contains(Database.SQL_SERVER);
		assertThat(Database.findDatabase(h2)).contains(Database.H2);
	}

	@Test // gh-20
	public void shouldNotResolveUnknownDatabase() {
		assertThat(Database.findDatabase(new UnknownConnectionFactory())).isEmpty();
	}

	static class UnknownConnectionFactory implements ConnectionFactory {

		@Override
		public Publisher<? extends Connection> create() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ConnectionFactoryMetadata getMetadata() {
			return () -> "foo";
		}
	}
}
