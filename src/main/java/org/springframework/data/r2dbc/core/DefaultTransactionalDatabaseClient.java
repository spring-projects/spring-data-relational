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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.connectionfactory.ConnectionFactoryUtils;
import org.springframework.data.r2dbc.connectionfactory.ReactiveTransactionSynchronization;
import org.springframework.data.r2dbc.connectionfactory.TransactionResources;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.transaction.NoTransactionException;

/**
 * Default implementation of a {@link TransactionalDatabaseClient}.
 *
 * @author Mark Paluch
 */
class DefaultTransactionalDatabaseClient extends DefaultDatabaseClient implements TransactionalDatabaseClient {

	DefaultTransactionalDatabaseClient(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
			ReactiveDataAccessStrategy dataAccessStrategy, NamedParameterExpander namedParameters,
			DefaultDatabaseClientBuilder builder) {
		super(connector, exceptionTranslator, dataAccessStrategy, namedParameters, builder);
	}

	@Override
	public TransactionalDatabaseClient.Builder mutate() {
		return (TransactionalDatabaseClient.Builder) super.mutate();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.TransactionalDatabaseClient#beginTransaction()
	 */
	@Override
	public Mono<Void> beginTransaction() {

		Mono<TransactionResources> transactional = ConnectionFactoryUtils.currentReactiveTransactionSynchronization() //
				.map(synchronization -> {

					TransactionResources transactionResources = TransactionResources.create();
					// TODO: This Tx management code creating a TransactionContext. Find a better place.
					synchronization.registerTransaction(transactionResources);
					return transactionResources;
				});

		return transactional.flatMap(it -> {
			return ConnectionFactoryUtils.doGetConnection(obtainConnectionFactory());
		}).flatMap(it -> Mono.from(it.getT1().beginTransaction()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.TransactionalDatabaseClient#commitTransaction()
	 */
	@Override
	public Mono<Void> commitTransaction() {
		return cleanup(Connection::commitTransaction);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.TransactionalDatabaseClient#rollbackTransaction()
	 */
	@Override
	public Mono<Void> rollbackTransaction() {
		return cleanup(Connection::rollbackTransaction);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.TransactionalDatabaseClient#inTransaction(java.util.function.Function)
	 */
	@Override
	public <T> Flux<T> inTransaction(Function<DatabaseClient, ? extends Publisher<? extends T>> callback) {

		return Flux.usingWhen(beginTransaction().thenReturn(this), callback, //
				DefaultTransactionalDatabaseClient::commitTransaction, //
				DefaultTransactionalDatabaseClient::rollbackTransaction, //
				DefaultTransactionalDatabaseClient::rollbackTransaction) //
				.subscriberContext(DefaultTransactionalDatabaseClient::withTransactionSynchronization);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClient#getConnection()
	 */
	@Override
	protected Mono<Connection> getConnection() {
		return ConnectionFactoryUtils.getConnection(obtainConnectionFactory()).map(Tuple2::getT1);
	}

	/**
	 * Execute a transactional cleanup. Also, deregister the current {@link TransactionResources synchronization} element.
	 */
	private static Mono<Void> cleanup(Function<Connection, ? extends Publisher<Void>> callback) {

		return ConnectionFactoryUtils.currentActiveReactiveTransactionSynchronization() //
				.flatMap(synchronization -> {

					TransactionResources currentSynchronization = synchronization.getCurrentTransaction();

					ConnectionFactory connectionFactory = currentSynchronization.getResource(ConnectionFactory.class);

					if (connectionFactory == null) {
						throw new NoTransactionException("No ConnectionFactory attached");
					}

					return Mono.from(connectionFactory.create())
							.flatMap(connection -> Mono.from(callback.apply(connection))
									.then(ConnectionFactoryUtils.releaseConnection(connection, connectionFactory))
									.then(ConnectionFactoryUtils.closeConnection(connection, connectionFactory))) // TODO: Is this rather
																																																// related to
																																																// TransactionContext
																																																// cleanup?
							.doFinally(s -> synchronization.unregisterTransaction(currentSynchronization));
				});
	}

	/**
	 * Potentially register a {@link ReactiveTransactionSynchronization} in the {@link Context} if no synchronization
	 * object is registered.
	 *
	 * @param context the subscriber context.
	 * @return subscriber context with a registered synchronization.
	 */
	static Context withTransactionSynchronization(Context context) {

		// associate synchronizer object to host transactional resources.
		// TODO: Should be moved to a better place.
		return context.put(ReactiveTransactionSynchronization.class,
				context.getOrDefault(ReactiveTransactionSynchronization.class, new ReactiveTransactionSynchronization()));
	}
}
