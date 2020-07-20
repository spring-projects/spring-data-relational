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
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;

/**
 * Resource holder wrapping a R2DBC {@link Connection}. {@link R2dbcTransactionManager} binds instances of this class to
 * the thread, for a specific {@link ConnectionFactory}.
 * <p>
 * Inherits rollback-only support for nested R2DBC transactions and reference count functionality from the base class.
 * <p>
 * Note: This is an SPI class, not intended to be used by applications.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see R2dbcTransactionManager
 * @see ConnectionFactoryUtils
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection} instead.
 */
@Deprecated
public class ConnectionHolder extends ResourceHolderSupport {

	@Nullable private ConnectionHandle connectionHandle;

	@Nullable private Connection currentConnection;

	private boolean transactionActive;

	/**
	 * Create a new ConnectionHolder for the given R2DBC {@link Connection}, wrapping it with a
	 * {@link SimpleConnectionHandle}, assuming that there is no ongoing transaction.
	 *
	 * @param connection the R2DBC {@link Connection} to hold
	 * @see SimpleConnectionHandle
	 * @see #ConnectionHolder(Connection, boolean)
	 */
	public ConnectionHolder(Connection connection) {
		this(connection, false);
	}

	/**
	 * Create a new ConnectionHolder for the given R2DBC {@link Connection}, wrapping it with a
	 * {@link SimpleConnectionHandle}.
	 *
	 * @param connection the R2DBC {@link Connection} to hold
	 * @param transactionActive whether the given {@link Connection} is involved in an ongoing transaction
	 * @see SimpleConnectionHandle
	 */
	public ConnectionHolder(Connection connection, boolean transactionActive) {

		this.connectionHandle = new SimpleConnectionHandle(connection);
		this.transactionActive = transactionActive;
	}

	/**
	 * Return the ConnectionHandle held by this ConnectionHolder.
	 */
	@Nullable
	public ConnectionHandle getConnectionHandle() {
		return this.connectionHandle;
	}

	/**
	 * Return whether this holder currently has a {@link Connection}.
	 */
	protected boolean hasConnection() {
		return (this.connectionHandle != null);
	}

	/**
	 * Set whether this holder represents an active, R2DBC-managed transaction.
	 *
	 * @see R2dbcTransactionManager
	 */
	protected void setTransactionActive(boolean transactionActive) {
		this.transactionActive = transactionActive;
	}

	/**
	 * Return whether this holder represents an active, R2DBC-managed transaction.
	 */
	protected boolean isTransactionActive() {
		return this.transactionActive;
	}

	/**
	 * Override the existing Connection handle with the given {@link Connection}. Reset the handle if given
	 * {@literal null}.
	 * <p>
	 * Used for releasing the {@link Connection} on suspend (with a {@literal null} argument) and setting a fresh
	 * {@link Connection} on resume.
	 */
	protected void setConnection(@Nullable Connection connection) {
		if (this.currentConnection != null) {
			if (this.connectionHandle != null) {
				this.connectionHandle.releaseConnection(this.currentConnection);
			}
			this.currentConnection = null;
		}
		if (connection != null) {
			this.connectionHandle = new SimpleConnectionHandle(connection);
		} else {
			this.connectionHandle = null;
		}
	}

	/**
	 * Return the current {@link Connection} held by this {@link ConnectionHolder}.
	 * <p>
	 * This will be the same {@link Connection} until {@code released} gets called on the {@link ConnectionHolder}, which
	 * will reset the held {@link Connection}, fetching a new {@link Connection} on demand.
	 *
	 * @see ConnectionHandle#getConnection()
	 * @see #released()
	 */
	public Connection getConnection() {

		Assert.notNull(this.connectionHandle, "Active Connection is required");
		if (this.currentConnection == null) {
			this.currentConnection = this.connectionHandle.getConnection();
		}
		return this.currentConnection;
	}

	/**
	 * Releases the current {@link Connection} held by this {@link ConnectionHolder}.
	 * <p>
	 * This is necessary for {@link ConnectionHandle}s that expect "Connection borrowing", where each returned
	 * {@link Connection} is only temporarily leased and needs to be returned once the data operation is done, to make the
	 * Connection available for other operations within the same transaction.
	 */
	@Override
	public void released() {
		super.released();
		if (!isOpen() && this.currentConnection != null) {
			if (this.connectionHandle != null) {
				this.connectionHandle.releaseConnection(this.currentConnection);
			}
			this.currentConnection = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.support.ResourceHolderSupport#clear()
	 */
	@Override
	public void clear() {
		super.clear();
		this.transactionActive = false;
	}
}
