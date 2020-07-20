/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;

/**
 * Extension of the {@code io.r2dbc.spi.ConnectionFactory} interface, to be implemented by special connection factories
 * that return R2DBC Connections in an unwrapped fashion.
 * <p>
 * Classes using this interface can query whether or not the {@link Connection} should be closed after an operation.
 * Spring's {@link ConnectionFactoryUtils} automatically perform such a check.
 *
 * @author Mark Paluch
 * @see ConnectionFactoryUtils#closeConnection
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection} instead.
 */
@Deprecated
public interface SmartConnectionFactory extends ConnectionFactory {

	/**
	 * Should we close this {@link io.r2dbc.spi.Connection}, obtained from this {@code io.r2dbc.spi.ConnectionFactory}?
	 * <p>
	 * Code that uses Connections from a SmartConnectionFactory should always perform a check via this method before
	 * invoking {@code close()}.
	 *
	 * @param connection the {@link io.r2dbc.spi.Connection} to check.
	 * @return whether the given {@link Connection} should be closed.
	 * @see io.r2dbc.spi.Connection#close()
	 */
	boolean shouldClose(Connection connection);
}
