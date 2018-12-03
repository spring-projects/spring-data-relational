package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import org.springframework.util.Assert;

/**
 * Enumeration of known Databases for offline {@link Dialect} resolution. R2DBC {@link io.r2dbc.spi.ConnectionFactory}
 * provides {@link io.r2dbc.spi.ConnectionFactoryMetadata metadata} that allows resolving an appropriate {@link Dialect}
 * if none was configured explicitly.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public enum Database {

	POSTGRES {
		@Override
		public String driverName() {
			return "PostgreSQL";
		}

		@Override
		public Dialect defaultDialect() {
			return PostgresDialect.INSTANCE;
		}
	},

	SQL_SERVER {
		@Override
		public String driverName() {
			return "Microsoft SQL Server";
		}

		@Override
		public Dialect defaultDialect() {
			return SqlServerDialect.INSTANCE;
		}
	},

	H2 {
		@Override
		public String driverName() {
			return "H2";
		}

		@Override
		public Dialect defaultDialect() {
			return H2Dialect.INSTANCE;
		}
	};

	/**
	 * Find a {@link Database} type using {@link ConnectionFactory} and its metadata.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return the resolved {@link Database} or {@link Optional#empty()} if the database type cannot be determined from
	 *         {@link ConnectionFactory}.
	 */
	public static Optional<Database> findDatabase(ConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		ConnectionFactoryMetadata metadata = connectionFactory.getMetadata();

		return Arrays.stream(values()).filter(it -> matches(metadata, it.driverName())).findFirst();
	}

	private static boolean matches(ConnectionFactoryMetadata metadata, String databaseType) {
		return metadata.getName().toLowerCase(Locale.ENGLISH).contains(databaseType.toLowerCase(Locale.ENGLISH));
	}

	/**
	 * Returns the driver name.
	 *
	 * @return the driver name.
	 * @see ConnectionFactoryMetadata#getName()
	 */
	public abstract String driverName();

	/**
	 * Returns the latest {@link Dialect} for the underlying database.
	 *
	 * @return the latest {@link Dialect} for the underlying database.
	 */
	public abstract Dialect defaultDialect();

}
