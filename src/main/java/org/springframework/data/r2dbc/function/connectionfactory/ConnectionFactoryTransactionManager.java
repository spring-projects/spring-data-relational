/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Mono;

import java.time.Duration;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.lang.Nullable;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.util.Assert;

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
 */
public class ConnectionFactoryTransactionManager extends AbstractReactiveTransactionManager
		implements InitializingBean {

	private ConnectionFactory connectionFactory;

	private boolean enforceReadOnly = false;

	/**
	 * Create a new @link ConnectionFactoryTransactionManager} instance. A ConnectionFactory has to be set to be able to
	 * use it.
	 *
	 * @see #setConnectionFactory
	 */
	public ConnectionFactoryTransactionManager() {}

	/**
	 * Create a new {@link ConnectionFactoryTransactionManager} instance.
	 *
	 * @param connectionFactory the R2DBC ConnectionFactory to manage transactions for
	 */
	public ConnectionFactoryTransactionManager(ConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	/**
	 * Set the R2DBC {@link ConnectionFactory} that this instance should manage transactions for.
	 * <p>
	 * This will typically be a locally defined {@link ConnectionFactory}, for example an connection pool.
	 * <p>
	 * The {@link ConnectionFactory} specified here should be the target {@link ConnectionFactory} to manage transactions
	 * for, not a TransactionAwareConnectionFactoryProxy. Only data access code may work with
	 * TransactionAwareConnectionFactoryProxy, while the transaction manager needs to work on the underlying target
	 * {@link ConnectionFactory}. If there's nevertheless a TransactionAwareConnectionFactoryProxy passed in, it will be
	 * unwrapped to extract its target {@link ConnectionFactory}.
	 * <p>
	 * <b>The {@link ConnectionFactory} passed in here needs to return independent {@link Connection}s.</b> The
	 * {@link Connection}s may come from a pool (the typical case), but the {@link ConnectionFactory} must not return
	 * scoped {@link Connection} or the like.
	 *
	 * @see TransactionAwareConnectionFactoryProxy
	 */
	public void setConnectionFactory(@Nullable ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Return the R2DBC {@link ConnectionFactory} that this instance manages transactions for.
	 */
	@Nullable
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	/**
	 * Obtain the {@link ConnectionFactory} for actual use.
	 *
	 * @return the {@link ConnectionFactory} (never {@code null})
	 * @throws IllegalStateException in case of no ConnectionFactory set
	 */
	protected ConnectionFactory obtainConnectionFactory() {
		ConnectionFactory connectionFactory = getConnectionFactory();
		Assert.state(connectionFactory != null, "No ConnectionFactory set");
		return connectionFactory;
	}

	/**
	 * Specify whether to enforce the read-only nature of a transaction (as indicated by
	 * {@link TransactionDefinition#isReadOnly()} through an explicit statement on the transactional connection: "SET
	 * TRANSACTION READ ONLY" as understood by Oracle, MySQL and Postgres.
	 * <p>
	 * The exact treatment, including any SQL statement executed on the connection, can be customized through through
	 * {@link #prepareTransactionalConnection}.
	 *
	 * @see #prepareTransactionalConnection
	 */
	public void setEnforceReadOnly(boolean enforceReadOnly) {
		this.enforceReadOnly = enforceReadOnly;
	}

	/**
	 * Return whether to enforce the read-only nature of a transaction through an explicit statement on the transactional
	 * connection.
	 *
	 * @see #setEnforceReadOnly
	 */
	public boolean isEnforceReadOnly() {
		return this.enforceReadOnly;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		if (getConnectionFactory() == null) {
			throw new IllegalArgumentException("Property 'connectionFactory' is required");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doGetTransaction(org.springframework.transaction.reactive.TransactionSynchronizationManager)
	 */
	@Override
	protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager)
			throws TransactionException {

		ConnectionFactoryTransactionObject txObject = new ConnectionFactoryTransactionObject();
		ConnectionHolder conHolder = (ConnectionHolder) synchronizationManager.getResource(obtainConnectionFactory());
		txObject.setConnectionHolder(conHolder, false);
		return txObject;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#isExistingTransaction(java.lang.Object)
	 */
	@Override
	protected boolean isExistingTransaction(Object transaction) {

		ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) transaction;
		return (txObject.hasConnectionHolder() && txObject.getConnectionHolder().isTransactionActive());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doBegin(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object, org.springframework.transaction.TransactionDefinition)
	 */
	@Override
	protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction,
			TransactionDefinition definition) throws TransactionException {

		ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) transaction;

		return Mono.defer(() -> {

			Mono<Connection> connection = null;

			if (!txObject.hasConnectionHolder() || txObject.getConnectionHolder().isSynchronizedWithTransaction()) {
				Mono<Connection> newCon = Mono.from(obtainConnectionFactory().create());

				connection = newCon.doOnNext(it -> {

					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Acquired Connection [" + newCon + "] for R2DBC transaction");
					}
					txObject.setConnectionHolder(new ConnectionHolder(it), true);
				});
			} else {
				txObject.getConnectionHolder().setSynchronizedWithTransaction(true);
				connection = Mono.just(txObject.getConnectionHolder().getConnection());
			}

			return connection.flatMap(con -> {

				return prepareTransactionalConnection(con, definition).then(doBegin(con, definition)).then().doOnSuccess(v -> {
					txObject.getConnectionHolder().setTransactionActive(true);

					Duration timeout = determineTimeout(definition);
					if (!timeout.isNegative() && !timeout.isZero()) {
						txObject.getConnectionHolder().setTimeoutInMillis(timeout.toMillis());
					}

					// Bind the connection holder to the thread.
					if (txObject.isNewConnectionHolder()) {
						synchronizationManager.bindResource(obtainConnectionFactory(), txObject.getConnectionHolder());
					}
				}).thenReturn(con).onErrorResume(e -> {

					CannotCreateTransactionException ex = new CannotCreateTransactionException(
							"Could not open R2DBC Connection for transaction", e);

					if (txObject.isNewConnectionHolder()) {
						return ConnectionFactoryUtils.releaseConnection(con, obtainConnectionFactory()).doOnTerminate(() -> {

							txObject.setConnectionHolder(null, false);
						}).then(Mono.error(ex));
					}
					return Mono.error(ex);
				});
			});
		}).then();
	}

	private Mono<Void> doBegin(Connection con, TransactionDefinition definition) {

		Mono<Void> doBegin = Mono.from(con.beginTransaction());

		if (definition != null && definition.getIsolationLevel() != -1) {

			IsolationLevel isolationLevel = resolveIsolationLevel(definition.getIsolationLevel());

			if (isolationLevel != null) {
				if (this.logger.isDebugEnabled()) {
					this.logger
							.debug("Changing isolation level of R2DBC Connection [" + con + "] to " + definition.getIsolationLevel());
				}
				doBegin = doBegin.then(Mono.from(con.setTransactionIsolationLevel(isolationLevel)));
			}
		}

		return doBegin;
	}

	/**
	 * Determine the actual timeout to use for the given definition. Will fall back to this manager's default timeout if
	 * the transaction definition doesn't specify a non-default value.
	 *
	 * @param definition the transaction definition
	 * @return the actual timeout to use
	 * @see org.springframework.transaction.TransactionDefinition#getTimeout()
	 */
	protected Duration determineTimeout(TransactionDefinition definition) {
		if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
			return Duration.ofSeconds(definition.getTimeout());
		}
		return Duration.ZERO;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doSuspend(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object)
	 */
	@Override
	protected Mono<Object> doSuspend(TransactionSynchronizationManager synchronizationManager, Object transaction)
			throws TransactionException {

		return Mono.defer(() -> {

			ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) transaction;
			txObject.setConnectionHolder(null);
			return Mono.justOrEmpty(synchronizationManager.unbindResource(obtainConnectionFactory()));
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doResume(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object, java.lang.Object)
	 */
	@Override
	protected Mono<Void> doResume(TransactionSynchronizationManager synchronizationManager, Object transaction,
			Object suspendedResources) throws TransactionException {

		return Mono.defer(() -> {

			ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) transaction;
			txObject.setConnectionHolder(null);
			synchronizationManager.bindResource(obtainConnectionFactory(), suspendedResources);

			return Mono.empty();
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doCommit(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected Mono<Void> doCommit(TransactionSynchronizationManager TransactionSynchronizationManager,
			GenericReactiveTransaction status) throws TransactionException {

		ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) status.getTransaction();
		Connection connection = txObject.getConnectionHolder().getConnection();
		if (status.isDebug()) {
			this.logger.debug("Committing R2DBC transaction on Connection [" + connection + "]");
		}

		return Mono.from(connection.commitTransaction())
				.onErrorMap(ex -> new TransactionSystemException("Could not commit R2DBC transaction", ex));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doRollback(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected Mono<Void> doRollback(TransactionSynchronizationManager TransactionSynchronizationManager,
			GenericReactiveTransaction status) throws TransactionException {

		ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) status.getTransaction();
		Connection connection = txObject.getConnectionHolder().getConnection();
		if (status.isDebug()) {
			this.logger.debug("Rolling back R2DBC transaction on Connection [" + connection + "]");
		}

		return Mono.from(connection.rollbackTransaction())
				.onErrorMap(ex -> new TransactionSystemException("Could not roll back R2DBC transaction", ex));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doSetRollbackOnly(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected Mono<Void> doSetRollbackOnly(TransactionSynchronizationManager synchronizationManager,
			GenericReactiveTransaction status) throws TransactionException {

		return Mono.fromRunnable(() -> {

			ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) status.getTransaction();

			if (status.isDebug()) {
				this.logger
						.debug("Setting R2DBC transaction [" + txObject.getConnectionHolder().getConnection() + "] rollback-only");
			}
			txObject.setRollbackOnly();
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doCleanupAfterCompletion(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object)
	 */
	@Override
	protected Mono<Void> doCleanupAfterCompletion(TransactionSynchronizationManager synchronizationManager,
			Object transaction) {

		return Mono.defer(() -> {

			ConnectionFactoryTransactionObject txObject = (ConnectionFactoryTransactionObject) transaction;

			// Remove the connection holder from the context, if exposed.
			if (txObject.isNewConnectionHolder()) {
				synchronizationManager.unbindResource(obtainConnectionFactory());
			}

			// Reset connection.
			Connection con = txObject.getConnectionHolder().getConnection();

			try {
				if (txObject.isNewConnectionHolder()) {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Releasing R2DBC Connection [" + con + "] after transaction");
					}
					return ConnectionFactoryUtils.releaseConnection(con, obtainConnectionFactory());
				}
			} finally {
				txObject.getConnectionHolder().clear();
			}

			return Mono.empty();
		});
	}

	/**
	 * Prepare the transactional {@link Connection} right after transaction begin.
	 * <p>
	 * The default implementation executes a "SET TRANSACTION READ ONLY" statement if the {@link #setEnforceReadOnly
	 * "enforceReadOnly"} flag is set to {@code true} and the transaction definition indicates a read-only transaction.
	 * <p>
	 * The "SET TRANSACTION READ ONLY" is understood by Oracle, MySQL and Postgres and may work with other databases as
	 * well. If you'd like to adapt this treatment, override this method accordingly.
	 *
	 * @param con the transactional R2DBC Connection
	 * @param definition the current transaction definition
	 * @see #setEnforceReadOnly
	 */
	protected Mono<Void> prepareTransactionalConnection(Connection con, TransactionDefinition definition) {

		if (isEnforceReadOnly() && definition.isReadOnly()) {

			return Mono.from(con.createStatement("SET TRANSACTION READ ONLY").execute()) //
					.flatMapMany(Result::getRowsUpdated) //
					.then();
		}

		return Mono.empty();
	}

	/**
	 * Resolve the {@link TransactionDefinition#getIsolationLevel() isolation level constant} to a R2DBC
	 * {@link IsolationLevel}. If you'd like to extend isolation level translation for vendor-specific
	 * {@link IsolationLevel}s, override this method accordingly.
	 *
	 * @param isolationLevel the isolation level to translate.
	 * @return the resolved isolation level. Can be {@literal null} if not resolvable or the isolation level should remain
	 *         {@link TransactionDefinition#ISOLATION_DEFAULT default}.
	 * @see TransactionDefinition#getIsolationLevel()
	 */
	@Nullable
	protected IsolationLevel resolveIsolationLevel(int isolationLevel) {

		switch (isolationLevel) {
			case TransactionDefinition.ISOLATION_READ_COMMITTED:
				return IsolationLevel.READ_COMMITTED;
			case TransactionDefinition.ISOLATION_READ_UNCOMMITTED:
				return IsolationLevel.READ_UNCOMMITTED;
			case TransactionDefinition.ISOLATION_REPEATABLE_READ:
				return IsolationLevel.REPEATABLE_READ;
			case TransactionDefinition.ISOLATION_SERIALIZABLE:
				return IsolationLevel.SERIALIZABLE;
		}

		return null;
	}

	/**
	 * ConnectionFactory transaction object, representing a ConnectionHolder. Used as transaction object by
	 * ConnectionFactoryTransactionManager.
	 */
	private static class ConnectionFactoryTransactionObject extends R2dbcTransactionObjectSupport {

		private boolean newConnectionHolder;

		void setConnectionHolder(@Nullable ConnectionHolder connectionHolder, boolean newConnectionHolder) {
			super.setConnectionHolder(connectionHolder);
			this.newConnectionHolder = newConnectionHolder;
		}

		boolean isNewConnectionHolder() {
			return this.newConnectionHolder;
		}

		void setRollbackOnly() {
			getConnectionHolder().setRollbackOnly();
		}
	}
}
