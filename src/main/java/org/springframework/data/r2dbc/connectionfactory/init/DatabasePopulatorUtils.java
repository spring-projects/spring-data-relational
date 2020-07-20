/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory.init;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import org.springframework.dao.DataAccessException;
import org.springframework.data.r2dbc.connectionfactory.ConnectionFactoryUtils;
import org.springframework.util.Assert;

/**
 * Utility methods for executing a {@link DatabasePopulator}.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@Deprecated
public abstract class DatabasePopulatorUtils {

	// utility constructor
	private DatabasePopulatorUtils() {}

	/**
	 * Execute the given {@link DatabasePopulator} against the given {@link io.r2dbc.spi.ConnectionFactory}.
	 *
	 * @param populator the {@link DatabasePopulator} to execute.
	 * @param connectionFactory the {@link ConnectionFactory} to execute against.
	 * @return {@link Mono} that initiates {@link DatabasePopulator#populate(Connection)} and is notified upon completion.
	 */
	public static Mono<Void> execute(DatabasePopulator populator, ConnectionFactory connectionFactory)
			throws DataAccessException {

		Assert.notNull(populator, "DatabasePopulator must not be null");
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		return Mono.usingWhen(ConnectionFactoryUtils.getConnection(connectionFactory), //
				populator::populate, //
				it -> ConnectionFactoryUtils.releaseConnection(it, connectionFactory), //
				(it, err) -> ConnectionFactoryUtils.releaseConnection(it, connectionFactory),
				it -> ConnectionFactoryUtils.releaseConnection(it, connectionFactory))
				.onErrorMap(ex -> !(ex instanceof ScriptException), ex -> {
					return new UncategorizedScriptException("Failed to execute database script", ex);
				});
	}
}
