package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.reactivestreams.Publisher;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/**
 * Unit tests for {@link DialectResolver}.
 *
 * @author Mark Paluch
 */
public class DialectResolverUnitTests {

	@Test // GH-20, GH-104, GH-1475
	void shouldResolveDatabaseType() {

		PostgresqlConnectionFactory postgres = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
				.host("localhost").database("foo").username("bar").password("password").build());
		H2ConnectionFactory h2 = new H2ConnectionFactory(H2ConnectionConfiguration.builder().inMemory("mem").build());
		MariadbConnectionFactory mariadb = new MariadbConnectionFactory(
				MariadbConnectionConfiguration.builder().socket("/foo").username("bar").build());
		MySqlConnectionFactory mysql = MySqlConnectionFactory
				.from(MySqlConnectionConfiguration.builder().host("foo").username("bar").build());

		assertThat(DialectResolver.getDialect(postgres)).isEqualTo(PostgresDialect.INSTANCE);
		assertThat(DialectResolver.getDialect(h2)).isEqualTo(H2Dialect.INSTANCE);
		assertThat(DialectResolver.getDialect(mariadb)).isEqualTo(MySqlDialect.INSTANCE);
		assertThat(DialectResolver.getDialect(mysql)).isEqualTo(MySqlDialect.INSTANCE);
	}

	@Test // gh-20, gh-104
	void shouldNotResolveUnknownDatabase() {
		assertThatThrownBy(() -> DialectResolver.getDialect(new ExternalConnectionFactory("unknown")))
				.isInstanceOf(DialectResolver.NoDialectException.class);
	}

	@Test // gh-104
	void shouldResolveExternalDialect() {
		assertThat(DialectResolver.getDialect(new ExternalConnectionFactory("external")))
				.isEqualTo(ExternalDialect.INSTANCE);
	}

	record ExternalConnectionFactory(String name) implements ConnectionFactory {

		@Override
		public Publisher<? extends Connection> create() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ConnectionFactoryMetadata getMetadata() {
			return () -> this.name;
		}
	}

	static class ExternalDialectProvider implements DialectResolver.R2dbcDialectProvider {

		@Override
		public Optional<R2dbcDialect> getDialect(ConnectionFactory connectionFactory) {

			if (connectionFactory.getMetadata().getName().equals("external")) {
				return Optional.of(ExternalDialect.INSTANCE);
			}
			return Optional.empty();
		}
	}

	enum ExternalDialect implements R2dbcDialect {

		INSTANCE;

		@Override
		public BindMarkersFactory getBindMarkersFactory() {
			return null;
		}

		@Override
		public LimitClause limit() {
			return null;
		}

		@Override
		public LockClause lock() {
			return new LockClause() {
				@Override
				public String getLock(LockOptions lockOptions) {
					return "FOR UPDATE";
				}

				@Override
				public Position getClausePosition() {
					return Position.AFTER_ORDER_BY;
				}
			};
		}

		@Override
		public SelectRenderContext getSelectContext() {
			return null;
		}
	}
}
