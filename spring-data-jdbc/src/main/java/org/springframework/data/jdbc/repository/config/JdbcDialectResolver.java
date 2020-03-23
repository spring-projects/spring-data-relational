/*
 * Copyright 2020 the original author or authors.
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

import java.awt.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.dialect.MySqlDialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.dialect.SqlServerDialect;
import org.springframework.data.util.Optionals;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Resolves a {@link Dialect} from a {@link DataSource} using {@link JdbcDialectProvider}. Dialect resolution uses
 * Spring's {@link SpringFactoriesLoader spring.factories} to determine available extensions.
 *
 * @author Jens Schauder
 * @since 2.0
 * @see Dialect
 * @see SpringFactoriesLoader
 */
public class JdbcDialectResolver {

	private static final List<JdbcDialectProvider> DETECTORS = SpringFactoriesLoader
			.loadFactories(JdbcDialectProvider.class, JdbcDialectResolver.class.getClassLoader());

	// utility constructor.
	private JdbcDialectResolver() {}

	/**
	 * Retrieve a {@link Dialect} by inspecting a {@link DataSource}.
	 *
	 * @param template must not be {@literal null}.
	 * @return the resolved {@link Dialect} {@link NoDialectException} if the database type cannot be determined from
	 *         {@link DataSource}.
	 * @throws NoDialectException if no {@link Dialect} can be found.
	 */
	public static Dialect getDialect(NamedParameterJdbcOperations template) {

		return DETECTORS.stream() //
				.map(it -> it.getDialect(template)) //
				.flatMap(Optionals::toStream) //
				.findFirst() //
				.orElseThrow(() -> new NoDialectException(
						String.format("Cannot determine a dialect for %s. Please provide a Dialect.",
								template)));
	}

	/**
	 * SPI to extend Spring's default JDBC Dialect discovery mechanism. Implementations of this interface are discovered
	 * through Spring's {@link SpringFactoriesLoader} mechanism.
	 *
	 * @author Jens Schauder
	 * @see org.springframework.core.io.support.SpringFactoriesLoader
	 */
	public interface JdbcDialectProvider {

		/**
		 * Returns a {@link Dialect} for a {@link DataSource}.
		 *
		 * @param template the {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate} to be used with the {@link Dialect}.
		 * @return {@link Optional} containing the {@link Dialect} if the {@link JdbcDialectProvider} can provide a dialect
		 *         object, otherwise {@link Optional#empty()}.
		 */
		Optional<Dialect> getDialect(NamedParameterJdbcOperations template);
	}

	static public class DefaultDialectProvider implements JdbcDialectProvider {

		@Override
		public Optional<Dialect> getDialect(NamedParameterJdbcOperations template) {

			return template.getJdbcOperations().execute((ConnectionCallback<Optional<Dialect>>) (connection) ->{

				DatabaseMetaData metaData = connection.getMetaData();

				String name = metaData.getDatabaseProductName().toLowerCase();

				if (name.contains("hsql")) {
					return Optional.of(HsqlDbDialect.INSTANCE);
				}
				if (name.contains("mysql")) { // catches also mariadb
					return Optional.of(MySqlDialect.INSTANCE);
				}
				if (name.contains("postgresql")) {
					return Optional.of(PostgresDialect.INSTANCE);
				}
				if (name.contains("microsoft")) {
					return Optional.of(SqlServerDialect.INSTANCE);
				}

				return Optional.empty();
			});

		}
	}

	/**
	 * Exception thrown when {@link JdbcDialectResolver} cannot resolve a {@link Dialect}.
	 */
	public static class NoDialectException extends NonTransientDataAccessException {

		/**
		 * Constructor for NoDialectFoundException.
		 *
		 * @param msg the detail message
		 */
		public NoDialectException(String msg) {
			super(msg);
		}
	}

}
