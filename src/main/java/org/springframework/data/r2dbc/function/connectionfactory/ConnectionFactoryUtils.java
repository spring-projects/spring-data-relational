/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.function.connectionfactory;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;

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
 * Used internally by Spring's {@link org.springframework.data.r2dbc.function.DatabaseClient}, Spring's R2DBC operation
 * objects. Can also be used directly in application code.
 *
 * @author Mark Paluch
 */
public abstract class ConnectionFactoryUtils {

	/**
	 * Order value for ReactiveTransactionSynchronization objects that clean up R2DBC Connections.
	 */
	public static final int CONNECTION_SYNCHRONIZATION_ORDER = 1000;

	private static final Log logger = LogFactory.getLog(ConnectionFactoryUtils.class);

	private ConnectionFactoryUtils() {

	}

	/**
	 * Obtain a {@link io.r2dbc.spi.Connection} from the given {@link io.r2dbc.spi.ConnectionFactory}. Translates
	 * exceptions into the Spring hierarchy of unchecked generic data access exceptions, simplifying calling code and
	 * making any exception that is thrown more meaningful.
	 * <p>
	 * Is aware of a corresponding Connection bound to the current {@link reactor.util.context.Context}. Will bind a
	 * Connection to the {@link reactor.util.context.Context} if transaction synchronization is active.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to obtain Connections from
	 * @return a R2DBC Connection from the given {@link io.r2dbc.spi.ConnectionFactory}.
	 * @throws DataAccessResourceFailureException if the attempt to get a {@link io.r2dbc.spi.Connection} failed
	 * @see #releaseConnection
	 */
	public static Mono<Tuple2<Connection, ConnectionFactory>> getConnection(ConnectionFactory connectionFactory) {
		return doGetConnection(connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to obtain R2DBC Connection", e));
	}

	/**
	 * Actually obtain a R2DBC Connection from the given {@link ConnectionFactory}. Same as {@link #getConnection}, but
	 * preserving the original exceptions.
	 * <p>
	 * Is aware of a corresponding Connection bound to the current {@link reactor.util.context.Context}. Will bind a
	 * Connection to the {@link reactor.util.context.Context} if transaction synchronization is active.
	 *
	 * @param connectionFactory the {@link ConnectionFactory} to obtain Connections from.
	 * @return a R2DBC {@link io.r2dbc.spi.Connection} from the given {@link ConnectionFactory}.
	 */
	public static Mono<Tuple2<Connection, ConnectionFactory>> doGetConnection(ConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		return TransactionSynchronizationManager.currentTransaction().flatMap(synchronizationManager -> {

			ConnectionHolder conHolder = (ConnectionHolder) synchronizationManager.getResource(connectionFactory);
			if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
				conHolder.requested();
				if (!conHolder.hasConnection()) {
					logger.debug("Fetching resumed R2DBC Connection from ConnectionFactory");
					return fetchConnection(connectionFactory).doOnNext(conHolder::setConnection);
				}
				return Mono.just(conHolder.getConnection());
			}
			// Else we either got no holder or an empty thread-bound holder here.

			logger.debug("Fetching R2DBC Connection from ConnectionFactory");
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
				.map(conn -> Tuples.of(conn, connectionFactory)) //
				.onErrorResume(NoTransactionException.class, e -> {

					return Mono.subscriberContext().flatMap(it -> {

						if (it.hasKey(ReactiveTransactionSynchronization.class)) {

							ReactiveTransactionSynchronization synchronization = it.get(ReactiveTransactionSynchronization.class);

							return obtainConnection(synchronization, connectionFactory);
						}
						return Mono.empty();
					}).switchIfEmpty(Mono.defer(() -> {
						return Mono.from(connectionFactory.create()).map(it -> Tuples.of(it, connectionFactory));
					}));
				});
	}

	private static Mono<Tuple2<Connection, ConnectionFactory>> obtainConnection(
			ReactiveTransactionSynchronization synchronization, ConnectionFactory connectionFactory) {

		if (synchronization.isSynchronizationActive()) {

			logger.debug("Registering transaction synchronization for R2DBC Connection");

			TransactionResources txContext = synchronization.getCurrentTransaction();
			ConnectionFactory resource = txContext.getResource(ConnectionFactory.class);

			Mono<Tuple2<Connection, ConnectionFactory>> attachNewConnection = Mono
					.defer(() -> Mono.from(connectionFactory.create()).map(it -> {

						logger.debug("Fetching new R2DBC Connection from ConnectionFactory");

						SingletonConnectionFactory s = new SingletonConnectionFactory(connectionFactory.getMetadata(), it);
						txContext.registerResource(ConnectionFactory.class, s);

						return Tuples.of(it, connectionFactory);
					}));

			return Mono.justOrEmpty(resource).flatMap(factory -> {

				logger.debug("Fetching resumed R2DBC Connection from ConnectionFactory");

				return Mono.from(factory.create())
						.map(connection -> Tuples.<Connection, ConnectionFactory> of(connection, factory));

			}).switchIfEmpty(attachNewConnection);
		}

		return Mono.empty();
	}

	/**
	 * Actually fetch a {@link Connection} from the given {@link ConnectionFactory}, defensively turning an unexpected
	 * {@code null} return value from {@link ConnectionFactory#create()} into an {@link IllegalStateException}.
	 *
	 * @param connectionFactory the {@link ConnectionFactory} to obtain {@link Connection}s from
	 * @return a R2DBC {@link Connection} from the given {@link ConnectionFactory} (never {@code null})
	 * @throws IllegalStateException if the {@link ConnectionFactory} returned a {@literal null} value.
	 * @see ConnectionFactory#create()
	 */
	private static Mono<Connection> fetchConnection(ConnectionFactory connectionFactory) {

		Publisher<? extends Connection> con = connectionFactory.create();
		if (con == null) {
			throw new IllegalStateException("ConnectionFactory returned null from getConnection(): " + connectionFactory);
		}
		return Mono.from(con);
	}

	/**
	 * Close the given {@link io.r2dbc.spi.Connection}, obtained from the given {@link ConnectionFactory}, if it is not
	 * managed externally (that is, not bound to the thread).
	 *
	 * @param con the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link ConnectionFactory} that the Connection was obtained from (may be
	 *          {@literal null}).
	 * @see #getConnection
	 */
	public static Mono<Void> releaseConnection(@Nullable io.r2dbc.spi.Connection con,
			@Nullable ConnectionFactory connectionFactory) {

		return doReleaseConnection(con, connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to close R2DBC Connection", e));
	}

	/**
	 * Actually close the given {@link io.r2dbc.spi.Connection}, obtained from the given {@link ConnectionFactory}. Same
	 * as {@link #releaseConnection}, but preserving the original exception.
	 *
	 * @param con the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link ConnectionFactory} that the Connection was obtained from (may be
	 *          {@literal null}).
	 * @see #doGetConnection
	 */
	public static Mono<Void> doReleaseConnection(@Nullable io.r2dbc.spi.Connection con,
			@Nullable ConnectionFactory connectionFactory) {

		return TransactionSynchronizationManager.currentTransaction().flatMap(it -> {

			ConnectionHolder conHolder = (ConnectionHolder) it.getResource(connectionFactory);
			if (conHolder != null && connectionEquals(conHolder, con)) {
				// It's the transactional Connection: Don't close it.
				conHolder.released();
			}
			return Mono.from(con.close());
		}).onErrorResume(NoTransactionException.class, e -> {

			if (connectionFactory instanceof SingletonConnectionFactory) {

				SingletonConnectionFactory factory = (SingletonConnectionFactory) connectionFactory;

				logger.debug("Releasing R2DBC Connection");

				return factory.close(con);
			}

			logger.debug("Closing R2DBC Connection");

			return Mono.from(con.close());
		});
	}

	/**
	 * Close the {@link io.r2dbc.spi.Connection}. Translates exceptions into the Spring hierarchy of unchecked generic
	 * data access exceptions, simplifying calling code and making any exception that is thrown more meaningful.
	 *
	 * @param connectionFactory the {@link io.r2dbc.spi.ConnectionFactory} to obtain Connections from
	 * @return a R2DBC Connection from the given {@link io.r2dbc.spi.ConnectionFactory}.
	 * @throws DataAccessResourceFailureException if the attempt to get a {@link io.r2dbc.spi.Connection} failed
	 */
	public static Mono<Void> closeConnection(Connection connection, ConnectionFactory connectionFactory) {
		return doCloseConnection(connection, connectionFactory)
				.onErrorMap(e -> new DataAccessResourceFailureException("Failed to obtain R2DBC Connection", e));
	}

	/**
	 * Close the {@link io.r2dbc.spi.Connection}, unless a {@link SmartConnectionFactory} doesn't want us to.
	 *
	 * @param connection the {@link io.r2dbc.spi.Connection} to close if necessary.
	 * @param connectionFactory the {@link ConnectionFactory} that the Connection was obtained from.
	 * @see Connection#close()
	 * @see SmartConnectionFactory#shouldClose(Connection)
	 */
	public static Mono<Void> doCloseConnection(Connection connection, @Nullable ConnectionFactory connectionFactory) {

		if (!(connectionFactory instanceof SingletonConnectionFactory)
				|| ((SingletonConnectionFactory) connectionFactory).shouldClose(connection)) {

			SingletonConnectionFactory factory = (SingletonConnectionFactory) connectionFactory;
			return factory.close(connection).then(Mono.from(connection.close()));
		}

		return Mono.empty();
	}

	/**
	 * Obtain the currently {@link ReactiveTransactionSynchronization} from the current subscriber
	 * {@link reactor.util.context.Context}.
	 *
	 * @see Mono#subscriberContext()
	 * @see ReactiveTransactionSynchronization
	 * @throws NoTransactionException if no active {@link ReactiveTransactionSynchronization} is associated with the
	 *           current subscription.
	 */
	public static Mono<ReactiveTransactionSynchronization> currentReactiveTransactionSynchronization() {

		return Mono.subscriberContext().filter(it -> it.hasKey(ReactiveTransactionSynchronization.class)) //
				.switchIfEmpty(Mono.error(new NoTransactionException(
						"Transaction management is not enabled. Make sure to register ReactiveTransactionSynchronization in the subscriber Context!"))) //
				.map(it -> it.get(ReactiveTransactionSynchronization.class));
	}

	/**
	 * Obtain the currently active {@link ReactiveTransactionSynchronization} from the current subscriber
	 * {@link reactor.util.context.Context}.
	 *
	 * @see Mono#subscriberContext()
	 * @see ReactiveTransactionSynchronization
	 * @throws NoTransactionException if no active {@link ReactiveTransactionSynchronization} is associated with the
	 *           current subscription.
	 */
	public static Mono<ReactiveTransactionSynchronization> currentActiveReactiveTransactionSynchronization() {

		return currentReactiveTransactionSynchronization()
				.filter(ReactiveTransactionSynchronization::isSynchronizationActive) //
				.switchIfEmpty(Mono.error(new NoTransactionException("ReactiveTransactionSynchronization not active!")));
	}

	/**
	 * Obtain the {@link io.r2dbc.spi.ConnectionFactory} from the current subscriber {@link reactor.util.context.Context}.
	 *
	 * @see Mono#subscriberContext()
	 * @see ReactiveTransactionSynchronization
	 * @see TransactionResources
	 */
	public static Mono<ConnectionFactory> currentConnectionFactory(ConnectionFactory connectionFactory) {

		return TransactionSynchronizationManager.currentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive).filter(it -> {

					ConnectionHolder conHolder = (ConnectionHolder) it.getResource(connectionFactory);
					if (conHolder != null && (conHolder.hasConnection() || conHolder.isSynchronizedWithTransaction())) {
						return true;
					}
					return false;
				}).map(it -> connectionFactory).onErrorResume(NoTransactionException.class, e -> {

					return currentActiveReactiveTransactionSynchronization().map(synchronization -> {

						TransactionResources currentSynchronization = synchronization.getCurrentTransaction();
						return currentSynchronization.getResource(ConnectionFactory.class);
					}).switchIfEmpty(Mono.error(new DataAccessResourceFailureException(
							"Cannot extract ConnectionFactory from current TransactionContext!")));

				});
	}

	/**
	 * Determine whether the given two {@link Connection}s are equal, asking the target {@link Connection} in case of a
	 * proxy. Used to detect equality even if the user passed in a raw target Connection while the held one is a proxy.
	 *
	 * @param conHolder the {@link ConnectionHolder} for the held Connection (potentially a proxy)
	 * @param passedInCon the {@link Connection} passed-in by the user (potentially a target {@link Connection} without
	 *          proxy)
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
	 * Return the innermost target {@link Connection} of the given {@link Connection}. If the given {@link Connection} is
	 * a proxy, it will be unwrapped until a non-proxy {@link Connection} is found. Otherwise, the passed-in Connection
	 * will be returned as-is.
	 *
	 * @param con the {@link Connection} proxy to unwrap
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
	 * Determine the connection synchronization order to use for the given {@link ConnectionFactory}. Decreased for every
	 * level of nesting that a {@link ConnectionFactory} has, checked through the level of
	 * {@link DelegatingConnectionFactory} nesting.
	 *
	 * @param connectionFactory the {@link ConnectionFactory} to check.
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

				return TransactionSynchronizationManager.currentTransaction().flatMap(it -> {

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
				return TransactionSynchronizationManager.currentTransaction().doOnNext(it -> {
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
				return TransactionSynchronizationManager.currentTransaction().flatMap(it -> {

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
				return TransactionSynchronizationManager.currentTransaction().flatMap(it -> {

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
