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
package org.springframework.data.r2dbc.connectionfactory.lookup;

import io.r2dbc.spi.ConnectionFactory;

/**
 * Strategy interface for looking up {@link ConnectionFactory} by name.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.lookup} instead.
 */
@FunctionalInterface
@Deprecated
public interface ConnectionFactoryLookup {

	/**
	 * Retrieve the {@link ConnectionFactory} identified by the given name.
	 *
	 * @param connectionFactoryName the name of the {@link ConnectionFactory}.
	 * @return the {@link ConnectionFactory} (never {@literal null}).
	 * @throws ConnectionFactoryLookupFailureException if the lookup failed.
	 */
	ConnectionFactory getConnectionFactory(String connectionFactoryName) throws ConnectionFactoryLookupFailureException;
}
