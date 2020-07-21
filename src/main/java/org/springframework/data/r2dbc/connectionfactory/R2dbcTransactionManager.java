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
import io.r2dbc.spi.ConnectionFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.r2dbc.core.DatabaseClient;

/**
 * {@link org.springframework.transaction.ReactiveTransactionManager} implementation for a single R2DBC
 * {@link ConnectionFactory}. This class is capable of working in any environment with any R2DBC driver, as long as the
 * setup uses a {@link ConnectionFactory} as its {@link Connection} factory mechanism. Binds a R2DBC {@link Connection}
 * from the specified {@link ConnectionFactory} to the current subscriber context, potentially allowing for one
 * context-bound {@link Connection} per {@link ConnectionFactory}.
 * <p>
 * <b>Note: The {@link ConnectionFactory} that this transaction manager operates on needs to return independent
 * {@link Connection}s.</b> The {@link Connection}s may come from a pool (the typical case), but the
 * {@link ConnectionFactory} must not return scoped scoped {@link Connection}s or the like. This transaction manager
 * will associate {@link Connection} with context-bound transactions itself, according to the specified propagation
 * behavior. It assumes that a separate, independent {@link Connection} can be obtained even during an ongoing
 * transaction.
 * <p>
 * Application code is required to retrieve the R2DBC Connection via
 * {@link ConnectionFactoryUtils#getConnection(ConnectionFactory)} instead of a standard R2DBC-style
 * {@link ConnectionFactory#create()} call. Spring classes such as {@link DatabaseClient} use this strategy implicitly.
 * If not used in combination with this transaction manager, the {@link ConnectionFactoryUtils} lookup strategy behaves
 * exactly like the native {@link ConnectionFactory} lookup; it can thus be used in a portable fashion.
 * <p>
 * Alternatively, you can allow application code to work with the standard R2DBC lookup pattern
 * {@link ConnectionFactory#create()}, for example for code that is not aware of Spring at all. In that case, define a
 * {@link TransactionAwareConnectionFactoryProxy} for your target {@link ConnectionFactory}, and pass that proxy
 * {@link ConnectionFactory} to your DAOs, which will automatically participate in Spring-managed transactions when
 * accessing it.
 * <p>
 * This transaction manager triggers flush callbacks on registered transaction synchronizations (if synchronization is
 * generally active), assuming resources operating on the underlying R2DBC {@link Connection}.
 *
 * @author Mark Paluch
 * @see ConnectionFactoryUtils#getConnection(ConnectionFactory)
 * @see ConnectionFactoryUtils#releaseConnection
 * @see TransactionAwareConnectionFactoryProxy
 * @see DatabaseClient
 * @deprecated since 1.2 in favor of Spring R2DBC. Use
 *             {@link org.springframework.r2dbc.connection.R2dbcTransactionManager} instead.
 */
@Deprecated
public class R2dbcTransactionManager extends org.springframework.r2dbc.connection.R2dbcTransactionManager
		implements InitializingBean {

	/**
	 * Create a new @link ConnectionFactoryTransactionManager} instance. A ConnectionFactory has to be set to be able to
	 * use it.
	 *
	 * @see #setConnectionFactory
	 */
	public R2dbcTransactionManager() {}

	/**
	 * Create a new {@link R2dbcTransactionManager} instance.
	 *
	 * @param connectionFactory the R2DBC ConnectionFactory to manage transactions for
	 */
	public R2dbcTransactionManager(ConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}
}
