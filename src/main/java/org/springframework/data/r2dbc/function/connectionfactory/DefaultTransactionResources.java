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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.Assert;

/**
 * Default implementation of {@link TransactionResources}.
 *
 * @author Mark Paluch
 */
class DefaultTransactionResources implements TransactionResources {

	private Map<Class<?>, Object> items = new ConcurrentHashMap<>();

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.connectionfactory.TransactionResources#registerResource(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <T> void registerResource(Class<T> key, T value) {

		Assert.state(!items.containsKey(key), () -> String.format("Resource for %s is already bound", key));

		items.put(key, value);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.connectionfactory.TransactionResources#getResource(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getResource(Class<T> key) {
		return (T) items.get(key);
	}
}
