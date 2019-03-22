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

import reactor.core.publisher.Mono;

/**
 * Transaction context for an ongoing transaction synchronization allowing to register transactional resources.
 * <p>
 * Supports one resource per key without overwriting, that is, a resource needs to be removed before a new one can be
 * set for the same key.
 * <p>
 * Primarily used by {@link ConnectionFactoryUtils} but can be also used by application code to register resources that
 * should be bound to a transaction.
 *
 * @author Mark Paluch
 */
public interface TransactionResources {

	/**
	 * Creates a new empty {@link TransactionResources}.
	 *
	 * @return the empty {@link TransactionResources}.
	 */
	static TransactionResources create() {
		return new DefaultTransactionResources();
	}

	/**
	 * Retrieve a resource from this context identified by {@code key}.
	 *
	 * @param key the resource key.
	 * @return the resource emitted through {@link Mono} or {@link Mono#empty()} if the resource was not found.
	 */
	<T> T getResource(Class<T> key);

	/**
	 * Register a resource in this context.
	 *
	 * @param key the resource key.
	 * @param value can be a subclass of the {@code key} type.
	 * @throws IllegalStateException if a resource is already bound under {@code key}.
	 */
	<T> void registerResource(Class<T> key, T value);
}
