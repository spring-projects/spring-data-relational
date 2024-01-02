/*
 * Copyright 2021-2024 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * {@link ConnectionFactoryProvider} for Oracle's R2DBC driver. Allows for absence of the driver which is required when
 * using Java 8 as the ServiceLoader requires presence of classes listed in the service loader manifest.
 *
 * @author Mark Paluch
 */
public class OracleConnectionFactoryProviderWrapper implements ConnectionFactoryProvider {

	private final ConnectionFactoryProvider delegate;

	public OracleConnectionFactoryProviderWrapper() {

		if (ClassUtils.isPresent("oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl", getClass().getClassLoader())) {

			delegate = createProvider();
		} else {
			delegate = null;
		}

	}

	private static ConnectionFactoryProvider createProvider() {

		try {
			return (ConnectionFactoryProvider) Class.forName("oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl")
					.getDeclaredConstructor().newInstance();
		} catch (ReflectiveOperationException e) {
			ReflectionUtils.handleReflectionException(e);
		}
		return null;
	}

	@Override
	public ConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
		if (delegate != null) {
			return delegate.create(connectionFactoryOptions);
		}
		throw new IllegalStateException(
				"Oracle R2DBC (oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl) is not on the class path");
	}

	@Override
	public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {

		if (delegate != null) {
			return delegate.supports(connectionFactoryOptions);
		}
		return false;
	}

	@Override
	public String getDriver() {

		if (delegate != null) {
			return delegate.getDriver();
		}

		return "oracle-r2dbc (proxy)";
	}

}
