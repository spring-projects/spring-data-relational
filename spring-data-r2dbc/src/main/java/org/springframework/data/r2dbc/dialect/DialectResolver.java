/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.util.Optionals;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Resolves a {@link R2dbcDialect} from a {@link ConnectionFactory} using {@link R2dbcDialectProvider}. Dialect
 * resolution uses Spring's {@link SpringFactoriesLoader spring.factories} to determine available extensions.
 *
 * @author Mark Paluch
 * @see R2dbcDialect
 * @see SpringFactoriesLoader
 */
public class DialectResolver {

	private static final List<R2dbcDialectProvider> DETECTORS = SpringFactoriesLoader
			.loadFactories(R2dbcDialectProvider.class, DialectResolver.class.getClassLoader());

	// utility constructor.
	private DialectResolver() {}

	/**
	 * Retrieve a {@link R2dbcDialect} by inspecting {@link ConnectionFactory} and its metadata.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return the resolved {@link R2dbcDialect} {@link NoDialectException} if the database type cannot be determined from
	 *         {@link ConnectionFactory}.
	 * @throws NoDialectException if no {@link R2dbcDialect} can be found.
	 */
	public static R2dbcDialect getDialect(ConnectionFactory connectionFactory) {

		return DETECTORS.stream() //
				.map(it -> it.getDialect(connectionFactory)) //
				.flatMap(Optionals::toStream) //
				.findFirst() //
				.orElseThrow(() -> {
					return new NoDialectException(
							String.format("Cannot determine a dialect for %s using %s; Please provide a Dialect",
									connectionFactory.getMetadata().getName(), connectionFactory));
				});
	}

	/**
	 * SPI to extend Spring's default R2DBC Dialect discovery mechanism. Implementations of this interface are discovered
	 * through Spring's {@link SpringFactoriesLoader} mechanism.
	 *
	 * @author Mark Paluch
	 * @see org.springframework.core.io.support.SpringFactoriesLoader
	 */
	public interface R2dbcDialectProvider {

		/**
		 * Returns a {@link R2dbcDialect} for a {@link ConnectionFactory}.
		 *
		 * @param connectionFactory the connection factory to be used with the {@link R2dbcDialect}.
		 * @return {@link Optional} containing the {@link R2dbcDialect} if the {@link R2dbcDialectProvider} can provide a
		 *         dialect object, otherwise {@link Optional#empty()}.
		 */
		Optional<R2dbcDialect> getDialect(ConnectionFactory connectionFactory);
	}

	/**
	 * Exception thrown when {@link DialectResolver} cannot resolve a {@link R2dbcDialect}.
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

	/**
	 * Built-in dialects. Used typically as last {@link R2dbcDialectProvider} when other providers register with a higher
	 * precedence.
	 *
	 * @see org.springframework.core.Ordered
	 * @see org.springframework.core.annotation.AnnotationAwareOrderComparator
	 */
	static class BuiltInDialectProvider implements R2dbcDialectProvider {

		private static final Map<String, R2dbcDialect> BUILTIN = new LinkedCaseInsensitiveMap<>(Locale.ENGLISH);

		static {
			BUILTIN.put("H2", H2Dialect.INSTANCE);
			BUILTIN.put("Microsoft SQL Server", SqlServerDialect.INSTANCE);
			BUILTIN.put("MySQL", MySqlDialect.INSTANCE);
			BUILTIN.put("MariaDB", MySqlDialect.INSTANCE);
			BUILTIN.put("Oracle", OracleDialect.INSTANCE);
			BUILTIN.put("PostgreSQL", PostgresDialect.INSTANCE);
		}

		@Override
		public Optional<R2dbcDialect> getDialect(ConnectionFactory connectionFactory) {

			ConnectionFactoryMetadata metadata = connectionFactory.getMetadata();
			R2dbcDialect r2dbcDialect = BUILTIN.get(metadata.getName());

			if (r2dbcDialect != null) {
				return Optional.of(r2dbcDialect);
			}

			return BUILTIN.keySet().stream() //
					.filter(it -> metadata.getName().contains(it)) //
					.map(BUILTIN::get) //
					.findFirst();
		}
	}
}
