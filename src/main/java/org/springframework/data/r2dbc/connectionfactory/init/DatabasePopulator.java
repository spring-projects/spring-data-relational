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
import reactor.core.publisher.Mono;

/**
 * Strategy used to populate, initialize, or clean up a database.
 *
 * @author Mark Paluch
 * @see ResourceDatabasePopulator
 * @see DatabasePopulatorUtils
 * @see ConnectionFactoryInitializer
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@FunctionalInterface
@Deprecated
public interface DatabasePopulator {

	/**
	 * Populate, initialize, or clean up the database using the provided R2DBC {@link Connection}.
	 *
	 * @param connection the R2DBC connection to use to populate the db; already configured and ready to use, must not be
	 *          {@literal null}.
	 * @return {@link Mono} that initiates script execution and is notified upon completion.
	 * @throws ScriptException in all other error cases
	 * @see DatabasePopulatorUtils#execute
	 */
	Mono<Void> populate(Connection connection) throws ScriptException;
}
