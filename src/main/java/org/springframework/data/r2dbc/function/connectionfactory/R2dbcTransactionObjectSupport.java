/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.spi.IsolationLevel;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Convenient base class for R2DBC-aware transaction objects. Can contain a {@link ConnectionHolder} with a R2DBC
 * {@link Connection}.
 *
 * @author Mark Paluch
 * @see ConnectionFactoryTransactionManager
 */
public abstract class R2dbcTransactionObjectSupport {

	@Nullable private ConnectionHolder connectionHolder;

	@Nullable private IsolationLevel previousIsolationLevel;

	private boolean savepointAllowed = false;

	public void setConnectionHolder(@Nullable ConnectionHolder connectionHolder) {
		this.connectionHolder = connectionHolder;
	}

	public ConnectionHolder getConnectionHolder() {
		Assert.state(this.connectionHolder != null, "No ConnectionHolder available");
		return this.connectionHolder;
	}

	public boolean hasConnectionHolder() {
		return (this.connectionHolder != null);
	}

	public void setPreviousIsolationLevel(@Nullable IsolationLevel previousIsolationLevel) {
		this.previousIsolationLevel = previousIsolationLevel;
	}

	@Nullable
	public IsolationLevel getPreviousIsolationLevel() {
		return this.previousIsolationLevel;
	}

	public void setSavepointAllowed(boolean savepointAllowed) {
		this.savepointAllowed = savepointAllowed;
	}

	public boolean isSavepointAllowed() {
		return this.savepointAllowed;
	}
}
