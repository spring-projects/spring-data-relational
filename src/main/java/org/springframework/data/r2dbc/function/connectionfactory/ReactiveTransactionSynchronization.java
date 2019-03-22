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

import java.util.Stack;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Central delegate that manages transactional resources. To be used by resource management code but not by typical
 * application code.
 * <p>
 * Supports a list of transactional resources if synchronization is active.
 * <p>
 * Resource management code should check for subscriber {@link reactor.util.context.Context}-bound resources, e.g. R2DBC
 * Connections using {@link TransactionResources#getResource(Class)}. Such code is normally not supposed to bind
 * resources, as this is the responsibility of transaction managers. A further option is to lazily bind on first use if
 * transaction synchronization is active, for performing transactions that span an arbitrary number of resources.
 * <p>
 * Transaction synchronization must be activated and deactivated by a transaction manager by registering
 * {@link ReactiveTransactionSynchronization} in the {@link reactor.util.context.Context subscriber context}.
 *
 * @author Mark Paluch
 */
public class ReactiveTransactionSynchronization {

	private Stack<TransactionResources> resources = new Stack<>();

	/**
	 * Return if transaction synchronization is active for the current {@link reactor.util.context.Context}. Can be called
	 * before register to avoid unnecessary instance creation.
	 */
	public boolean isSynchronizationActive() {
		return !resources.isEmpty();
	}

	/**
	 * Create a new transaction span and register a {@link TransactionResources} instance.
	 *
	 * @param transactionResources must not be {@literal null}.
	 */
	public void registerTransaction(TransactionResources transactionResources) {

		Assert.notNull(transactionResources, "TransactionContext must not be null!");

		resources.push(transactionResources);
	}

	/**
	 * Unregister a transaction span and by removing {@link TransactionResources} instance.
	 *
	 * @param transactionResources must not be {@literal null}.
	 */
	public void unregisterTransaction(TransactionResources transactionResources) {

		Assert.notNull(transactionResources, "TransactionContext must not be null!");

		resources.remove(transactionResources);
	}

	/**
	 * @return obtain the current {@link TransactionResources} or {@literal null} if none is present.
	 */
	@Nullable
	public TransactionResources getCurrentTransaction() {

		if (!resources.isEmpty()) {
			return resources.peek();
		}

		return null;
	}
}
