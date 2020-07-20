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
import reactor.core.publisher.Mono;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.Ordered;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Helper class that provides static methods for obtaining R2DBC Connections from a
 * {@link io.r2dbc.spi.ConnectionFactory}.
 * <p>
 * Used internally by Spring's {@link org.springframework.data.r2dbc.core.DatabaseClient}, Spring's R2DBC operation
 * objects. Can also be used directly in application code.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection} instead.
 */
@Deprecated
public abstract class ConnectionFactoryUtils {

	/**
	 * Order value for ReactiveTransactionSynchronization objects that clean up R2DBC Connections.
	 */
	public static final int CONNECTION_SYNCHRONIZATION_ORDER = 1000;

	private static final Log logger = LogFactory.getLog(ConnectionFactoryUtils.class);

	private ConnectionFactoryUtils() {}

	/**
	 * Obtain a {@link io.r2dbc.spi.Connection} from the given {@link io.r2dbc.spi.ConnectionFactory}. Translates
	 * exceptions into the Spring hierarchy of unchecked generic data access exceptions, simplifying calling code and
	 * making any exception that is thrown more meaningful.
	 * <p>
	 * Is aware of a corresponding Connection bound to the current {@link reactor.util.context.Context}. Will bind a
	 * Connection to the {@link reactor.util.context.Context} if transaction synchronization is active.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to obtain {@link io.r2dbc.spi.Connection
	 *          Connections} from.
	 * @return a R2DBC Connection from the given {@link io.r2dbc.spi.ConnectionFactory}.
	 * @throws DataAccessResourceFailureException if the attempt to get a {@link io.r2dbc.spi.Connection} failed.
	 * @see #releaseConnection
	 */
	public static Mono<Connection> getConnection(ConnectionFactory connectionFactory) {
		return doGetConnection(connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to obtain R2DBC Connection", e));
	}

	/**
	 * Actually obtain a R2DBC Connection from the given {@link io.r2dbc.spi.ConnectionFactory}. Same as
	 * {@link #getConnection}, but preserving the original exceptions.
	 * <p>
	 * Is aware of a corresponding Connection bound to the current {@link reactor.util.context.Context}. Will bind a
	 * Connection to the {@link reactor.util.context.Context} if transaction synchronization is active.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to obtain Connections from.
	 * @return a R2DBC {@link io.r2dbc.spi.Connection} from the given {@link io.r2dbc.spi.ConnectionFactory}.
	 */
	public static Mono<Connection> doGetConnection(ConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		return TransactionSynchronizationManager.forCurrentTransaction().flatMap(synchronizationManager -> {

			ConnectionHolder conHolder = (ConnectionHolder) synchronizationManager.getResource(connectionFactory);
			if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
				conHolder.requested();
				if (!conHolder.hasConnection()) {

					if (logger.isDebugEnabled()) {
						logger.debug("Fetching resumed R2DBC Connection from ConnectionFactory");
					}
					return fetchConnection(connectionFactory).doOnNext(conHolder::setConnection);
				}
				return Mono.just(conHolder.getConnection());
			}
			// Else we either got no holder or an empty thread-bound holder here.

			if (logger.isDebugEnabled()) {
				logger.debug("Fetching R2DBC Connection from ConnectionFactory");
			}

			Mono<Connection> con = fetchConnection(connectionFactory);

			if (synchronizationManager.isSynchronizationActive()) {

				return con.flatMap(it -> {

					return Mono.just(it).doOnNext(conn -> {

						// Use same Connection for further R2DBC actions within the transaction.
						// Thread-bound object will get removed by synchronization at transaction completion.
						ConnectionHolder holderToUse = conHolder;
						if (holderToUse == null) {
							holderToUse = new ConnectionHolder(conn);
						} else {
							holderToUse.setConnection(conn);
						}
						holderToUse.requested();
						synchronizationManager
								.registerSynchronization(new ConnectionSynchronization(holderToUse, connectionFactory));
						holderToUse.setSynchronizedWithTransaction(true);
						if (holderToUse != conHolder) {
							synchronizationManager.bindResource(connectionFactory, holderToUse);
						}
					}).onErrorResume(e -> {
						// Unexpected exception from external delegation call -> close Connection and rethrow.
						return releaseConnection(it, connectionFactory).then(Mono.error(e));
					});
				});
			}

			return con;
		}) //
				.onErrorResume(NoTransactionException.class, e -> {
					return Mono.from(connectionFactory.create());
				});
	}

	/**
	 * Actually fetch a {@link io.r2dbc.spi.Connection} from the given {@link io.r2dbc.spi.ConnectionFactory}, defensively
	 * turning an unexpected {@literal null} return value from {@link io.r2dbc.spi.ConnectionFactory#create()} into an
	 * {@link IllegalStateException}.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to obtain {@link io.r2dbc.spi.Connection}s from
	 * @return a R2DBC {@link io.r2dbc.spi.Connection} from the given {@link io.r2dbc.spi.ConnectionFactory} (never
	 *         {@literal null}).
	 * @throws IllegalStateException if the {@link io.r2dbc.spi.ConnectionFactory} returned a {@literal null} value.
	 * @see ConnectionFactory#create()
	 */
	private static Mono<Connection> fetchConnection(ConnectionFactory connectionFactory) {
		return Mono.from(connectionFactory.create());
	}

	/**
	 * Close the given {@link io.r2dbc.spi.Connection}, obtained from the given {@link io.r2dbc.spi.ConnectionFactory}, if
	 * it is not managed externally (that is, not bound to the thread).
	 *
	 * @param con the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} that the Connection was obtained from.
	 * @see #getConnection
	 */
	public static Mono<Void> releaseConnection(io.r2dbc.spi.Connection con, ConnectionFactory connectionFactory) {

		return doReleaseConnection(con, connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to close R2DBC Connection", e));
	}

	/**
	 * Actually close the given {@link io.r2dbc.spi.Connection}, obtained from the given
	 * {@link io.r2dbc.spi.ConnectionFactory}. Same as {@link #releaseConnection}, but preserving the original exception.
	 *
	 * @param connection the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} that the Connection was obtained from.
	 * @see #doGetConnection
	 */
	public static Mono<Void> doReleaseConnection(io.r2dbc.spi.Connection connection,
			ConnectionFactory connectionFactory) {

		return TransactionSynchronizationManager.forCurrentTransaction().flatMap(it -> {

			ConnectionHolder conHolder = (ConnectionHolder) it.getResource(connectionFactory);
			if (conHolder != null && connectionEquals(conHolder, connection)) {
				// It's the transactional Connection: Don't close it.
				conHolder.released();
			}
			return Mono.from(connection.close());
		}).onErrorResume(NoTransactionException.class, e -> {
			return doCloseConnection(connection, connectionFactory);
		});
	}

	/**
	 * Close the {@link io.r2dbc.spi.Connection}. Translates exceptions into the Spring hierarchy of unchecked generic
	 * data access exceptions, simplifying calling code and making any exception that is thrown more meaningful.
	 *
	 * @param connection the {@link io.r2dbc.spi.Connection} to close.
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} that the {@link io.r2dbc.spi.Connection} was
	 *          obtained from.
	 * @return a R2DBC Connection from the given {@link io.r2dbc.spi.ConnectionFactory}.
	 * @throws DataAccessResourceFailureException if the attempt to get a {@link io.r2dbc.spi.Connection} failed
	 */
	public static Mono<Void> closeConnection(Connection connection, ConnectionFactory connectionFactory) {

		Assert.notNull(connection, "Connection must not be null!");
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		return doCloseConnection(connection, connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to obtain R2DBC Connection", e));
	}

	/**
	 * Close the {@link io.r2dbc.spi.Connection}, unless a {@link SmartConnectionFactory} doesn't want us to.
	 *
	 * @param connection the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} that the Connection was obtained from.
	 * @see Connection#close()
	 * @see SmartConnectionFactory#shouldClose(Connection)
	 */
	public static Mono<Void> doCloseConnection(Connection connection, @Nullable ConnectionFactory connectionFactory) {

		if (!(connectionFactory instanceof SmartConnectionFactory)
				|| ((SmartConnectionFactory) connectionFactory).shouldClose(connection)) {

			if (logger.isDebugEnabled()) {
				logger.debug("Closing R2DBC Connection");
			}

			return Mono.from(connection.close());
		}

		return Mono.empty();
	}

	/**
	 * Obtain the {@link io.r2dbc.spi.ConnectionFactory} from the current subscriber {@link reactor.util.context.Context}.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} that the Connection was obtained from.
	 * @see TransactionSynchronizationManager
	 */
	public static Mono<ConnectionFactory> currentConnectionFactory(ConnectionFactory connectionFactory) {

		return TransactionSynchronizationManager.forCurrentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive).filter(it -> {

					ConnectionHolder conHolder = (ConnectionHolder) it.getResource(connectionFactory);
					if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
						return true;
					}
					return false;
				}).map(it -> connectionFactory);
	}

	/**
	 * Determine whether the given two {@link io.r2dbc.spi.Connection}s are equal, asking the target
	 * {@link io.r2dbc.spi.Connection} in case of a proxy. Used to detect equality even if the user passed in a raw target
	 * Connection while the held one is a proxy.
	 *
	 * @param conHolder the {@link .ConnectionHolder} for the held {@link io.r2dbc.spi.Connection} (potentially a proxy).
	 * @param passedInCon the {@link io.r2dbc.spi.Connection} passed-in by the user (potentially a target
	 *          {@link io.r2dbc.spi.Connection} without proxy).
	 * @return whether the given Connections are equal
	 * @see #getTargetConnection
	 */
	private static boolean connectionEquals(ConnectionHolder conHolder, Connection passedInCon) {

		if (!conHolder.hasConnection()) {
			return false;
		}
		Connection heldCon = conHolder.getConnection();
		// Explicitly check for identity too: for Connection handles that do not implement
		// "equals" properly).
		return (heldCon == passedInCon || heldCon.equals(passedInCon) || getTargetConnection(heldCon).equals(passedInCon));
	}

	/**
	 * Return the innermost target {@link io.r2dbc.spi.Connection} of the given {@link io.r2dbc.spi.Connection}. If the
	 * given {@link io.r2dbc.spi.Connection} is a proxy, it will be unwrapped until a non-proxy
	 * {@link io.r2dbc.spi.Connection} is found. Otherwise, the passed-in Connection will be returned as-is.
	 *
	 * @param con the {@link io.r2dbc.spi.Connection} proxy to unwrap
	 * @return the innermost target Connection, or the passed-in one if no proxy
	 * @see ConnectionProxy#getTargetConnection()
	 */
	public static Connection getTargetConnection(Connection con) {

		Connection conToUse = con;
		while (conToUse instanceof ConnectionProxy) {
			conToUse = ((ConnectionProxy) conToUse).getTargetConnection();
		}
		return conToUse;
	}

	/**
	 * Determine the connection synchronization order to use for the given {@link io.r2dbc.spi.ConnectionFactory}.
	 * Decreased for every level of nesting that a {@link io.r2dbc.spi.ConnectionFactory} has, checked through the level
	 * of {@link DelegatingConnectionFactory} nesting.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to check.
	 * @return the connection synchronization order to use.
	 * @see #CONNECTION_SYNCHRONIZATION_ORDER
	 */
	private static int getConnectionSynchronizationOrder(ConnectionFactory connectionFactory) {

		int order = CONNECTION_SYNCHRONIZATION_ORDER;
		ConnectionFactory current = connectionFactory;
		while (current instanceof DelegatingConnectionFactory) {
			order--;
			current = ((DelegatingConnectionFactory) current).getTargetConnectionFactory();
		}
		return order;
	}

	/**
	 * Callback for resource cleanup at the end of a non-native R2DBC transaction.
	 */
	private static class ConnectionSynchronization implements TransactionSynchronization, Ordered {

		private final ConnectionHolder connectionHolder;

		private final ConnectionFactory connectionFactory;

		private int order;

		private boolean holderActive = true;

		ConnectionSynchronization(ConnectionHolder connectionHolder, ConnectionFactory connectionFactory) {
			this.connectionHolder = connectionHolder;
			this.connectionFactory = connectionFactory;
			this.order = getConnectionSynchronizationOrder(connectionFactory);
		}

		@Override
		public int getOrder() {
			return this.order;
		}

		@Override
		public Mono<Void> suspend() {
			if (this.holderActive) {

				return TransactionSynchronizationManager.forCurrentTransaction().flatMap(it -> {

					it.unbindResource(this.connectionFactory);
					if (this.connectionHolder.hasConnection() && !this.connectionHolder.isOpen()) {
						// Release Connection on suspend if the application doesn't keep
						// a handle to it anymore. We will fetch a fresh Connection if the
						// application accesses the ConnectionHolder again after resume,
						// assuming that it will participate in the same transaction.
						return releaseConnection(this.connectionHolder.getConnection(), this.connectionFactory)
								.doOnTerminate(() -> this.connectionHolder.setConnection(null));
					}
					return Mono.empty();
				});
			}

			return Mono.empty();
		}

		@Override
		public Mono<Void> resume() {
			if (this.holderActive) {
				return TransactionSynchronizationManager.forCurrentTransaction().doOnNext(it -> {
					it.bindResource(this.connectionFactory, this.connectionHolder);
				}).then();
			}
			return Mono.empty();
		}

		@Override
		public Mono<Void> beforeCompletion() {

			// Release Connection early if the holder is not open anymore
			// (that is, not used by another resource
			// that has its own cleanup via transaction synchronization),
			// to avoid issues with strict transaction implementations that expect
			// the close call before transaction completion.
			if (!this.connectionHolder.isOpen()) {
				return TransactionSynchronizationManager.forCurrentTransaction().flatMap(it -> {

					it.unbindResource(this.connectionFactory);
					this.holderActive = false;
					if (this.connectionHolder.hasConnection()) {
						return releaseConnection(this.connectionHolder.getConnection(), this.connectionFactory);
					}
					return Mono.empty();
				});
			}

			return Mono.empty();
		}

		@Override
		public Mono<Void> afterCompletion(int status) {

			// If we haven't closed the Connection in beforeCompletion,
			// close it now. The holder might have been used for other
			// cleanup in the meantime, for example by a Hibernate Session.
			if (this.holderActive) {
				// The thread-bound ConnectionHolder might not be available anymore,
				// since afterCompletion might get called from a different thread.
				return TransactionSynchronizationManager.forCurrentTransaction().flatMap(it -> {

					it.unbindResourceIfPossible(this.connectionFactory);
					this.holderActive = false;
					if (this.connectionHolder.hasConnection()) {
						return releaseConnection(this.connectionHolder.getConnection(), this.connectionFactory)
								.doOnTerminate(() -> {
									// Reset the ConnectionHolder: It might remain bound to the context.
									this.connectionHolder.setConnection(null);
								});
					}

					return Mono.empty();
				});
			}

			this.connectionHolder.reset();
			return Mono.empty();
		}
	}
}
