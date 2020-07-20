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

import org.springframework.dao.NonTransientDataAccessException;

/**
 * Exception to be thrown by a {@link ConnectionFactoryLookup} implementation, indicating that the specified
 * {@link io.r2dbc.spi.ConnectionFactory} could not be obtained.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.lookup} instead.
 */
@SuppressWarnings("serial")
@Deprecated
public class ConnectionFactoryLookupFailureException extends NonTransientDataAccessException {

	/**
	 * Constructor for {@link ConnectionFactoryLookupFailureException}.
	 *
	 * @param msg the detail message.
	 */
	public ConnectionFactoryLookupFailureException(String msg) {
		super(msg);
	}

	/**
	 * Constructor for {@link ConnectionFactoryLookupFailureException}.
	 *
	 * @param msg the detail message.
	 * @param cause the root cause.
	 */
	public ConnectionFactoryLookupFailureException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
