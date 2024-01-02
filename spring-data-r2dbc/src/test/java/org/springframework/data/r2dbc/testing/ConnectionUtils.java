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
package org.springframework.data.r2dbc.testing;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;

import javax.sql.DataSource;

/**
 * Utility methods to configure {@link DataSource}/{@link ConnectionFactoryOptions}.
 *
 * @author Mark Paluch
 */
abstract class ConnectionUtils {

	public static String AARCH64 = "aarch64";

	/**
	 * Obtain a {@link ConnectionFactory} given {@link ExternalDatabase} and {@code driver}.
	 *
	 * @param driver
	 * @param configuration
	 * @return
	 */
	static ConnectionFactory getConnectionFactory(String driver, ExternalDatabase configuration) {
		return ConnectionFactories.get(createOptions(driver, configuration));
	}

	/**
	 * Create {@link ConnectionFactoryOptions} from {@link ExternalDatabase} and {@code driver}.
	 *
	 * @param driver
	 * @param configuration
	 * @return
	 */
	static ConnectionFactoryOptions createOptions(String driver, ExternalDatabase configuration) {

		return ConnectionFactoryOptions.builder().option(DRIVER, driver) //
				.option(USER, configuration.getUsername()) //
				.option(PASSWORD, configuration.getPassword()) //
				.option(DATABASE, configuration.getDatabase()) //
				.option(HOST, configuration.getHostname()) //
				.option(PORT, configuration.getPort()) //
				.build();
	}

	private ConnectionUtils() {
		// utility constructor.
	}
}
