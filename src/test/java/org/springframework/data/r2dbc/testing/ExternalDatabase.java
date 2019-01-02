/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.testing;

import lombok.Builder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.junit.AssumptionViolatedException;
import org.junit.rules.ExternalResource;

/**
 * {@link ExternalResource} wrapper to encapsulate {@link ProvidedDatabase} and
 * {@link org.testcontainers.containers.PostgreSQLContainer}.
 *
 * @author Mark Paluch
 */
public abstract class ExternalDatabase extends ExternalResource {

	/**
	 * @return the post of the database service.
	 */
	public abstract int getPort();

	/**
	 * @return hostname on which the database service runs.
	 */
	public abstract String getHostname();

	/**
	 * @return name of the database.
	 */
	public abstract String getDatabase();

	/**
	 * @return database user name.
	 */
	public abstract String getUsername();

	@Override
	protected void before() {

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(getHostname(), getPort()), Math.toIntExact(TimeUnit.SECONDS.toMillis(5)));

		} catch (IOException e) {
			throw new AssumptionViolatedException(
					String.format("Cannot connect to %s:%d. Skipping tests.", getHostname(), getPort()));
		}
	}

	/**
	 * @return password for the database user.
	 */
	public abstract String getPassword();

	/**
	 * Provided (unmanaged resource) database connection coordinates.
	 */
	@Builder
	public static class ProvidedDatabase extends ExternalDatabase {

		private final int port;
		private final String hostname;
		private final String database;
		private final String username;
		private final String password;

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getPort()
		 */
		@Override
		public int getPort() {
			return port;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getHostname()
		 */
		@Override
		public String getHostname() {
			return hostname;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.core.function.ExternalDatabase#getDatabase()
		 */
		@Override
		public String getDatabase() {
			return database;
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
	}
}
