/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.function.connectionfactory.TransactionResources;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.util.Assert;

/**
 * {@link DatabaseClient} that participates in an ongoing transaction if the subscription happens within a hosted
 * transaction. Alternatively, transactions can be started and cleaned up using {@link #beginTransaction()} and
 * {@link #commitTransaction()}.
 * <p>
 * Transactional resources are bound to {@link ReactiveTransactionSynchronization} through nested
 * {@link TransactionContext} enabling nested (parallel) transactions. The simplemost approach to use transactions is by
 * using {@link #inTransaction(Function)} which will start a transaction and commit it on successful termination. The
 * callback allows execution of multiple statements within the same transaction.
 *
 * <pre class="code">
 * Flux<Integer> transactionalFlux = databaseClient.inTransaction(db -> {
 *
 * 	return db.execute().sql("INSERT INTO person (id, firstname, lastname) VALUES(:id, :firstname, :lastname)") //
 * 			.bind("id", 1) //
 * 			.bind("firstname", "Walter") //
 * 			.bind("lastname", "White") //
 * 			.fetch().rowsUpdated();
 * });
 * </pre>
 *
 * Alternatively, transactions can be controlled by using {@link #beginTransaction()} and {@link #commitTransaction()}
 * methods. This approach requires {@link #enableTransactionSynchronization(Publisher) enabling of transaction
 * synchronization} for the transactional operation.
 *
 * <pre class="code">
 * Mono<Void> mono = databaseClient.beginTransaction()
 * 		.then(databaseClient.execute()
 * 				.sql("INSERT INTO person (id, firstname, lastname) VALUES(:id, :firstname, :lastname)") //
 * 				.bind("id", 1) //
 * 				.bind("firstname", "Walter") //
 * 				.bind("lastname", "White") //
 * 				.fetch().rowsUpdated())
 * 		.then(databaseClient.commitTransaction());
 *
 * Mono<Void> transactionalMono = databaseClient.enableTransactionSynchronization(mono);
 * </pre>
 * <p>
 * This {@link DatabaseClient} can be safely used without transaction synchronization to invoke database functionality
 * in auto-commit transactions.
 *
 * @author Mark Paluch
 * @see #inTransaction(Function)
 * @see #enableTransactionSynchronization(Publisher)
 * @see #beginTransaction()
 * @see #commitTransaction()
 * @see #rollbackTransaction()
 * @see org.springframework.data.r2dbc.function.connectionfactory.ReactiveTransactionSynchronization
 * @see TransactionResources
 * @see org.springframework.data.r2dbc.function.connectionfactory.ConnectionFactoryUtils
 */
public interface TransactionalDatabaseClient extends DatabaseClient {

	/**
	 * Start a transaction and bind connection resources to the subscriber context.
	 *
	 * @return
	 */
	Mono<Void> beginTransaction();

	/**
	 * Commit a transaction and unbind connection resources from the subscriber context.
	 *
	 * @return
	 * @throws org.springframework.transaction.NoTransactionException if no transaction is ongoing.
	 */
	Mono<Void> commitTransaction();

	/**
	 * Rollback a transaction and unbind connection resources from the subscriber context.
	 *
	 * @return
	 * @throws org.springframework.transaction.NoTransactionException if no transaction is ongoing.
	 */
	Mono<Void> rollbackTransaction();

	/**
	 * Execute a {@link Function} accepting a {@link DatabaseClient} within a managed transaction. {@link Exception Error
	 * signals} cause the transaction to be rolled back.
	 *
	 * @param callback
	 * @return the callback result.
	 */
	<T> Flux<T> inTransaction(Function<DatabaseClient, ? extends Publisher<? extends T>> callback);

	/**
	 * Enable transaction management so that connections can be bound to the subscription.
	 *
	 * @param publisher must not be {@literal null}.
	 * @return the Transaction-enabled {@link Mono}.
	 */
	default <T> Mono<T> enableTransactionSynchronization(Mono<T> publisher) {

		Assert.notNull(publisher, "Publisher must not be null!");

		return publisher.subscriberContext(DefaultTransactionalDatabaseClient::withTransactionSynchronization);
	}

	/**
	 * Enable transaction management so that connections can be bound to the subscription.
	 *
	 * @param publisher must not be {@literal null}.
	 * @return the Transaction-enabled {@link Flux}.
	 */
	default <T> Flux<T> enableTransactionSynchronization(Publisher<T> publisher) {

		Assert.notNull(publisher, "Publisher must not be null!");

		return Flux.from(publisher).subscriberContext(DefaultTransactionalDatabaseClient::withTransactionSynchronization);
	}

	/**
	 * Return a builder to mutate properties of this database client.
	 */
	TransactionalDatabaseClient.Builder mutate();

	// Static, factory methods

	/**
	 * A variant of {@link #create(ConnectionFactory)} that accepts a {@link io.r2dbc.spi.ConnectionFactory}.
	 */
	static TransactionalDatabaseClient create(ConnectionFactory factory) {
		return (TransactionalDatabaseClient) new DefaultTransactionalDatabaseClientBuilder().connectionFactory(factory)
				.build();
	}

	/**
	 * Obtain a {@code DatabaseClient} builder.
	 */
	static TransactionalDatabaseClient.Builder builder() {
		return new DefaultTransactionalDatabaseClientBuilder();
	}

	/**
	 * A mutable builder for creating a {@link TransactionalDatabaseClient}.
	 */
	interface Builder extends DatabaseClient.Builder {

		/**
		 * Configures the {@link ConnectionFactory R2DBC connector}.
		 *
		 * @param factory must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder connectionFactory(ConnectionFactory factory);

		/**
		 * Configures a {@link R2dbcExceptionTranslator}.
		 *
		 * @param exceptionTranslator must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder exceptionTranslator(R2dbcExceptionTranslator exceptionTranslator);

		/**
		 * Configures a {@link ReactiveDataAccessStrategy}.
		 *
		 * @param accessStrategy must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder dataAccessStrategy(ReactiveDataAccessStrategy accessStrategy);

		/**
		 * Configures {@link NamedParameterExpander}.
		 *
		 * @param expander must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 * @see NamedParameterExpander#enabled()
		 * @see NamedParameterExpander#disabled()
		 */
		Builder namedParameters(NamedParameterExpander expander);

		/**
		 * Configures a {@link Consumer} to configure this builder.
		 *
		 * @param builderConsumer must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder apply(Consumer<DatabaseClient.Builder> builderConsumer);

		@Override
		TransactionalDatabaseClient build();
	}
}
