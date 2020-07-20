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
import io.r2dbc.spi.Wrapped;

/**
 * Sub interface of {@link Connection} to be implemented by Connection proxies. Allows access to the underlying target
 * Connection.
 * <p/>
 * This interface can be checked when there is a need to cast to a native R2DBC {@link Connection}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @deprecated since 1.2 in favor of Spring R2DBC. Use R2DBC's {@link Wrapped} mechanism instead.
 */
@Deprecated
public interface ConnectionProxy extends Connection, Wrapped<Connection> {

	/**
	 * Return the target {@link Connection} of this proxy.
	 * <p/>
	 * This will typically be the native driver {@link Connection} or a wrapper from a connection pool.
	 *
	 * @return the underlying Connection (never {@literal null})
	 * @throws IllegalStateException in case the connection has already been closed.
	 */
	Connection getTargetConnection();
}
