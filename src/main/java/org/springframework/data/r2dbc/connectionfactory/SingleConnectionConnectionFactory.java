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
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link SmartConnectionFactory} that wraps a single R2DBC Connection which is not closed after use.
 * Obviously, this is not multi-threading capable.
 * <p>
 * Note that at shutdown, someone should close the underlying Connection via the {@code close()} method. Client code
 * will never call close on the Connection handle if it is SmartDataSource-aware (e.g. uses
 * {@link ConnectionFactoryUtils#releaseConnection(io.r2dbc.spi.Connection, ConnectionFactory)}).
 * <p>
 * If client code will call {@link Connection#close()} in the assumption of a pooled Connection, like when using
 * persistence tools, set "suppressClose" to "true". This will return a close-suppressing proxy instead of the physical
 * Connection.
 * <p>
 * This is primarily intended for testing. For example, it enables easy testing outside an application server, for code
 * that expects to work on a {@link ConnectionFactory}.
 *
 * @author Mark Paluch
 * @see #create()
 * @see io.r2dbc.spi.Connection#close()
 * @see ConnectionFactoryUtils#releaseConnection(io.r2dbc.spi.Connection, ConnectionFactory)
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection} instead.
 */
@Deprecated
public class SingleConnectionConnectionFactory extends DelegatingConnectionFactory
		implements SmartConnectionFactory, DisposableBean {

	/** Create a close-suppressing proxy?. */
	private boolean suppressClose;

	/** Override auto-commit state?. */
	private @Nullable Boolean autoCommit;

	/** Wrapped Connection. */
	private final AtomicReference<Connection> target = new AtomicReference<>();

	/** Proxy Connection. */
	private @Nullable Connection connection;

	private final Mono<? extends Connection> connectionEmitter;

	/**
	 * Constructor for bean-style configuration.
	 */
	public SingleConnectionConnectionFactory(ConnectionFactory targetConnectionFactory) {
		super(targetConnectionFactory);
		this.connectionEmitter = super.create().cache();
	}

	/**
	 * Create a new {@link SingleConnectionConnectionFactory} using a R2DBC connection URL.
	 *
	 * @param url the R2DBC URL to use for accessing {@link ConnectionFactory} discovery.
	 * @param suppressClose if the returned {@link Connection} should be a close-suppressing proxy or the physical
	 *          {@link Connection}.
	 * @see ConnectionFactories#get(String)
	 */
	public SingleConnectionConnectionFactory(String url, boolean suppressClose) {
		super(ConnectionFactories.get(url));
		this.suppressClose = suppressClose;
		this.connectionEmitter = super.create().cache();
	}

	/**
	 * Create a new {@link SingleConnectionConnectionFactory} with a given {@link Connection} and
	 * {@link ConnectionFactoryMetadata}.
	 *
	 * @param target underlying target {@link Connection}.
	 * @param metadata {@link ConnectionFactory} metadata to be associated with this {@link ConnectionFactory}.
	 * @param suppressClose if the {@link Connection} should be wrapped with a {@link Connection} that suppresses
	 *          {@code close()} calls (to allow for normal {@code close()} usage in applications that expect a pooled
	 *          {@link Connection} but do not know our {@link SmartConnectionFactory} interface).
	 */
	public SingleConnectionConnectionFactory(Connection target, ConnectionFactoryMetadata metadata,
			boolean suppressClose) {
		super(new ConnectionFactory() {
			@Override
			public Publisher<? extends Connection> create() {
				return Mono.just(target);
			}

			@Override
			public ConnectionFactoryMetadata getMetadata() {
				return metadata;
			}
		});
		Assert.notNull(target, "Connection must not be null");
		Assert.notNull(metadata, "ConnectionFactoryMetadata must not be null");
		this.target.set(target);
		this.connectionEmitter = Mono.just(target);
		this.suppressClose = suppressClose;
		this.connection = (suppressClose ? getCloseSuppressingConnectionProxy(target) : target);
	}

	/**
	 * Set whether the returned {@link Connection} should be a close-suppressing proxy or the physical {@link Connection}.
	 */
	public void setSuppressClose(boolean suppressClose) {
		this.suppressClose = suppressClose;
	}

	/**
	 * Return whether the returned {@link Connection} will be a close-suppressing proxy or the physical
	 * {@link Connection}.
	 */
	protected boolean isSuppressClose() {
		return this.suppressClose;
	}

	/**
	 * Set whether the returned {@link Connection}'s "autoCommit" setting should be overridden.
	 */
	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	/**
	 * Return whether the returned {@link Connection}'s "autoCommit" setting should be overridden.
	 *
	 * @return the "autoCommit" value, or {@code null} if none to be applied
	 */
	@Nullable
	protected Boolean getAutoCommitValue() {
		return this.autoCommit;
	}

	@Override
	public Mono<? extends Connection> create() {

		Connection connection = this.target.get();

		return connectionEmitter.map(it -> {

			if (connection == null) {
				this.target.compareAndSet(connection, it);
				this.connection = (isSuppressClose() ? getCloseSuppressingConnectionProxy(it) : it);
			}

			return this.connection;
		}).flatMap(this::prepareConnection);
	}

	/**
	 * This is a single Connection: Do not close it when returning to the "pool".
	 */
	@Override
	public boolean shouldClose(Connection con) {
		return (con != this.connection && con != this.target.get());
	}

	/**
	 * Close the underlying {@link Connection}. The provider of this {@link ConnectionFactory} needs to care for proper
	 * shutdown.
	 * <p>
	 * As this bean implements {@link DisposableBean}, a bean factory will automatically invoke this on destruction of its
	 * cached singletons.
	 */
	@Override
	public void destroy() {
		resetConnection().block();
	}

	/**
	 * Reset the underlying shared Connection, to be reinitialized on next access.
	 */
	public Mono<Void> resetConnection() {

		Connection connection = this.target.get();

		if (connection == null) {
			return Mono.empty();
		}

		return Mono.defer(() -> {

			if (this.target.compareAndSet(connection, null)) {

				this.connection = null;

				return Mono.from(connection.close());
			}

			return Mono.empty();
		});
	}

	/**
	 * Prepare the {@link Connection} before using it. Applies {@link #getAutoCommitValue() auto-commit} settings if
	 * configured.
	 *
	 * @param connection the requested {@link Connection}.
	 * @return the prepared {@link Connection}.
	 */
	protected Mono<Connection> prepareConnection(Connection connection) {

		Boolean autoCommit = getAutoCommitValue();
		if (autoCommit != null) {
			return Mono.from(connection.setAutoCommit(autoCommit)).thenReturn(connection);
		}

		return Mono.just(connection);
	}

	/**
	 * Wrap the given {@link Connection} with a proxy that delegates every method call to it but suppresses close calls.
	 *
	 * @param target the original {@link Connection} to wrap.
	 * @return the wrapped Connection.
	 */
	protected Connection getCloseSuppressingConnectionProxy(Connection target) {
		return (Connection) Proxy.newProxyInstance(ConnectionProxy.class.getClassLoader(),
				new Class<?>[] { ConnectionProxy.class }, new CloseSuppressingInvocationHandler(target));
	}

	/**
	 * Invocation handler that suppresses close calls on R2DBC Connections.
	 *
	 * @see io.r2dbc.spi.Connection#close()
	 */
	private static class CloseSuppressingInvocationHandler implements InvocationHandler {

		private final io.r2dbc.spi.Connection target;

		CloseSuppressingInvocationHandler(io.r2dbc.spi.Connection target) {
			this.target = target;
		}

		@Override
		@Nullable
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// Invocation on ConnectionProxy interface coming in...

			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return proxy == args[0];
			} else if (method.getName().equals("hashCode")) {
				// Use hashCode of PersistenceManager proxy.
				return System.identityHashCode(proxy);
			} else if (method.getName().equals("unwrap")) {
				return target;
			} else if (method.getName().equals("close")) {
				// Handle close method: suppress, not valid.
				return Mono.empty();
			} else if (method.getName().equals("getTargetConnection")) {
				// Handle getTargetConnection method: return underlying Connection.
				return this.target;
			}

			// Invoke method on target Connection.
			try {
				Object retVal = method.invoke(this.target, args);

				return retVal;
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

}
