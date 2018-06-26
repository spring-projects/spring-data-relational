/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.NoTransactionException;
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
public class ConnectionFactoryUtils {

	private static final Log logger = LogFactory.getLog(ConnectionFactoryUtils.class);

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

		return Mono.subscriberContext().flatMap(it -> {

			if (it.hasKey(ReactiveTransactionSynchronization.class)) {

				ReactiveTransactionSynchronization synchronization = it.get(ReactiveTransactionSynchronization.class);

				return obtainConnection(synchronization, connectionFactory);
			}
			return Mono.empty();
		}).switchIfEmpty(Mono.defer(() -> {
			return Mono.from(connectionFactory.create()).map(it -> Tuples.of(it, connectionFactory));
		}));
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

		if (connectionFactory instanceof SingletonConnectionFactory) {

			SingletonConnectionFactory factory = (SingletonConnectionFactory) connectionFactory;

			logger.debug("Releasing R2DBC Connection");

			return factory.close(con);
		}

		logger.debug("Closing R2DBC Connection");

		return Mono.from(con.close());
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
	public static Mono<ConnectionFactory> currentConnectionFactory() {

		return currentActiveReactiveTransactionSynchronization() //
				.map(synchronization -> {

					TransactionResources currentSynchronization = synchronization.getCurrentTransaction();
					return currentSynchronization.getResource(ConnectionFactory.class);
				}).switchIfEmpty(Mono.error(new DataAccessResourceFailureException(
						"Cannot extract ConnectionFactory from current TransactionContext!")));
	}
}
