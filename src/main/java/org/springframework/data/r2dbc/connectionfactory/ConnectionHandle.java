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
package org.springframework.data.r2dbc.connectionfactory;

import io.r2dbc.spi.Connection;

/**
 * Simple interface to be implemented by handles for a R2DBC Connection.
 *
 * @author Mark Paluch
 * @see SimpleConnectionHandle
 * @see ConnectionHolder
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection} instead.
 */
@FunctionalInterface
@Deprecated
public interface ConnectionHandle {

	/**
	 * Fetch the R2DBC Connection that this handle refers to.
	 */
	Connection getConnection();

	/**
	 * Release the R2DBC Connection that this handle refers to. Assumes a non-blocking implementation without
	 * synchronization.
	 * <p>
	 * The default implementation is empty, assuming that the lifecycle of the connection is managed externally.
	 *
	 * @param connection the R2DBC Connection to release
	 */
	default void releaseConnection(Connection connection) {
	}
}
