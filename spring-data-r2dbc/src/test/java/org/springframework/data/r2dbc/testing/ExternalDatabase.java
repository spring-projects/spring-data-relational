/*
 * Copyright 2018-2024 the original author or authors.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * {@link BeforeAllCallback} wrapper to encapsulate {@link ProvidedDatabase} and {@link JdbcDatabaseContainer}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public abstract class ExternalDatabase implements BeforeAllCallback {

	private static final Logger LOG = LoggerFactory.getLogger(ExternalDatabase.class);

	/**
	 * Construct an absent database that is used as {@literal null} object if no database is available.
	 *
	 * @return an absent database.
	 */
	public static ExternalDatabase unavailable() {
		return NoAvailableDatabase.INSTANCE;
	}

	/**
	 * @return hostname on which the database service runs.
	 */
	public abstract String getHostname();

	/**
	 * @return the post of the database service.
	 */
	public abstract int getPort();

	/**
	 * @return database user name.
	 */
	public abstract String getUsername();

	/**
	 * @return password for the database user.
	 */
	public abstract String getPassword();

	/**
	 * @return name of the database.
	 */
	public abstract String getDatabase();

	/**
	 * @return JDBC URL for the endpoint.
	 */
	public abstract String getJdbcUrl();

	/**
	 * Throws an {@link TestAbortedException} if the database cannot be reached.
	 */
	@Override
	public void beforeAll(ExtensionContext context) {

		if (!checkValidity()) {
			throw new TestAbortedException(String.format("Cannot connect to %s. Skipping tests.", this));
		}
	}

	/**
	 * Performs a test if the database can actually be reached.
	 *
	 * @return true, if the database could be reached.
	 */
	boolean checkValidity() {

		try (Socket socket = new Socket()) {

			socket.connect(new InetSocketAddress(getHostname(), getPort()), Math.toIntExact(TimeUnit.SECONDS.toMillis(5)));
			return true;

		} catch (IOException e) {
			LOG.debug("external database not available.", e);
		}

		return false;
	}

	/**
	 * Provided (unmanaged resource) database connection coordinates.
	 */
	public static class ProvidedDatabase extends ExternalDatabase {

		private final String hostname;
		private final int port;
		private final String username;
		private final String password;
		private final String database;
		private final String jdbcUrl;

		public ProvidedDatabase(String hostname, int port, String username, String password, String database,
				String jdbcUrl) {
			this.hostname = hostname;
			this.port = port;
			this.username = username;
			this.password = password;
			this.database = database;
			this.jdbcUrl = jdbcUrl;
		}

		public static ProvidedDatabaseBuilder builder() {
			return new ProvidedDatabaseBuilder();
		}

		/**
		 * Create a {@link ProvidedDatabaseBuilder} initialized with {@link JdbcDatabaseContainer}.
		 *
		 * @param container
		 * @return
		 */
		public static ProvidedDatabaseBuilder builder(JdbcDatabaseContainer container) {

			return builder().hostname(container.getHost()) //
					.port(container.getFirstMappedPort()) //
					.username(container.getUsername()) //
					.password(container.getPassword()) //
					.jdbcUrl(container.getJdbcUrl());
		}

		/**
		 * Create a {@link ProvidedDatabase} from {@link JdbcDatabaseContainer}.
		 *
		 * @param container
		 * @return
		 */
		public static ProvidedDatabase from(JdbcDatabaseContainer container) {
			return builder(container).build();
		}

		/* (non-Javadoc)
		* @see org.springframework.data.jdbc.core.function.ExternalDatabase#getHostname()
		*/
		@Override
		public String getHostname() {
			return hostname;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPort()
		 */
		@Override
		public int getPort() {
			return port;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getUsername()
		 */
		@Override
		public String getUsername() {
			return username;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPassword()
		 */
		@Override
		public String getPassword() {
			return password;
		}

		/* (non-Javadoc)
		* @see org.springframework.data.jdbc.core.function.ExternalDatabase#getDatabase()
		*/
		@Override
		public String getDatabase() {
			return database;
		}

		/* (non-Javadoc)
		* @see org.springframework.data.jdbc.core.function.ExternalDatabase#getJdbcUrl()
		*/
		@Override
		public String getJdbcUrl() {
			return jdbcUrl;
		}
	}

	/**
	 * An {@link ExternalDatabase} that couldn't get constructed.
	 *
	 * @author Jens Schauder
	 */
	private static class NoAvailableDatabase extends ExternalDatabase {

		private static final NoAvailableDatabase INSTANCE = new NoAvailableDatabase();

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPort()
		 */
		@Override
		boolean checkValidity() {
			return false;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getHostname()
		 */
		@Override
		public String getHostname() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPort()
		 */
		@Override
		public int getPort() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getUsername()
		 */
		@Override
		public String getUsername() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPassword()
		 */
		@Override
		public String getPassword() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getDatabase()
		 */
		@Override
		public String getDatabase() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}

		/* (non-Javadoc)
		* @see org.springframework.data.jdbc.core.function.ExternalDatabase#getJdbcUrl()
		*/
		@Override
		public String getJdbcUrl() {
			throw new UnsupportedOperationException(getClass().getSimpleName());
		}
	}

	static class ProvidedDatabaseBuilder {
		private String hostname;
		private int port;
		private String username;
		private String password;
		private String database;
		private String jdbcUrl;

		public ProvidedDatabaseBuilder hostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public ProvidedDatabaseBuilder port(Integer port) {
			this.port = port;
			return this;
		}

		public ProvidedDatabaseBuilder username(String username) {
			this.username = username;
			return this;
		}

		public ProvidedDatabaseBuilder password(String password) {
			this.password = password;
			return this;
		}

		public ProvidedDatabaseBuilder database(String database) {
			this.database = database;
			return this;
		}
		public ProvidedDatabaseBuilder jdbcUrl(String jdbcUrl) {
			this.jdbcUrl = jdbcUrl;
			return this;
		}

		public ProvidedDatabase build() {
			return new ProvidedDatabase(hostname, port, username, password, database, jdbcUrl);
		}
	}
}
