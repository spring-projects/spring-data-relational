/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import java.sql.Connection;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Resolves a {@link Dialect}. Resolution typically uses {@link JdbcOperations} to obtain and inspect a
 * {@link Connection}. Dialect resolution uses Spring's {@link SpringFactoriesLoader spring.factories} to determine
 * available {@link JdbcDialectProvider extensions}.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 2.0
 * @see Dialect
 * @see SpringFactoriesLoader
 * @deprecated since 3.5, replacement {@link org.springframework.data.jdbc.core.dialect.DialectResolver} was moved to
 *             the {@link org.springframework.data.jdbc.core.dialect} package.
 */
@Deprecated(since = "3.5", forRemoval = true)
public class DialectResolver {

	// utility constructor.
	private DialectResolver() {}

	/**
	 * Retrieve a {@link Dialect} by inspecting a {@link Connection}.
	 *
	 * @param operations must not be {@literal null}.
	 * @return the resolved {@link Dialect} {@link NoDialectException} if the database type cannot be determined from
	 *         {@link DataSource}.
	 * @throws NoDialectException if no {@link Dialect} can be found.
	 */
	public static JdbcDialect getDialect(JdbcOperations operations) {
		return org.springframework.data.jdbc.core.dialect.DialectResolver.getDialect(operations);
	}

	/**
	 * SPI to extend Spring's default JDBC Dialect discovery mechanism. Implementations of this interface are discovered
	 * through Spring's {@link SpringFactoriesLoader} mechanism.
	 *
	 * @author Jens Schauder
	 * @see org.springframework.core.io.support.SpringFactoriesLoader
	 * @deprecated since 3.5, replacement {@link org.springframework.data.jdbc.core.dialect.DialectResolver} was moved to
	 *             the {@link org.springframework.data.jdbc.core.dialect} package.
	 */
	@Deprecated(since = "3.5", forRemoval = true)
	public interface JdbcDialectProvider
			extends org.springframework.data.jdbc.core.dialect.DialectResolver.JdbcDialectProvider {

		/**
		 * Returns a {@link Dialect} for a {@link DataSource}.
		 *
		 * @param operations the {@link JdbcOperations} to be used with the {@link Dialect}.
		 * @return {@link Optional} containing the {@link Dialect} if the {@link JdbcDialectProvider} can provide a dialect
		 *         object, otherwise {@link Optional#empty()}.
		 */
		Optional<Dialect> getDialect(JdbcOperations operations);
	}

	@Deprecated(since = "3.5", forRemoval = true)
	static public class DefaultDialectProvider extends
			org.springframework.data.jdbc.core.dialect.DialectResolver.DefaultDialectProvider implements JdbcDialectProvider {

	}

	/**
	 * Exception thrown when {@link DialectResolver} cannot resolve a {@link Dialect}.
	 */
	@Deprecated(since = "3.5", forRemoval = true)
	public static class NoDialectException
			extends org.springframework.data.jdbc.core.dialect.DialectResolver.NoDialectException {

		/**
		 * Constructor for NoDialectFoundException.
		 *
		 * @param msg the detail message
		 */
		NoDialectException(String msg) {
			super(msg);
		}
	}

}
