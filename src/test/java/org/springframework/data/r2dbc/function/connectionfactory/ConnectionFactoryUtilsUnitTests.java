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

import static org.mockito.Mockito.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import org.junit.Test;
import org.springframework.transaction.NoTransactionException;

/**
 * Unit tests for {@link ConnectionFactoryUtils}.
 *
 * @author Mark Paluch
 */
public class ConnectionFactoryUtilsUnitTests {

	@Test
	public void currentReactiveTransactionSynchronizationShouldReportSynchronization() {

		ConnectionFactoryUtils.currentReactiveTransactionSynchronization() //
				.subscriberContext(
						it -> it.put(ReactiveTransactionSynchronization.class, new ReactiveTransactionSynchronization()))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test
	public void currentReactiveTransactionSynchronizationShouldFailWithoutTxMgmt() {

		ConnectionFactoryUtils.currentReactiveTransactionSynchronization() //
				.as(StepVerifier::create) //
				.expectError(NoTransactionException.class) //
				.verify();
	}

	@Test
	public void currentActiveReactiveTransactionSynchronizationShouldReportSynchronization() {

		ConnectionFactoryUtils.currentActiveReactiveTransactionSynchronization() //
				.subscriberContext(it -> {
					ReactiveTransactionSynchronization sync = new ReactiveTransactionSynchronization();
					sync.registerTransaction(TransactionResources.create());
					return it.put(ReactiveTransactionSynchronization.class, sync);
				}).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test
	public void currentActiveReactiveTransactionSynchronization() {

		ConnectionFactoryUtils.currentActiveReactiveTransactionSynchronization() //
				.subscriberContext(
						it -> it.put(ReactiveTransactionSynchronization.class, new ReactiveTransactionSynchronization()))
				.as(StepVerifier::create) //
				.expectError(NoTransactionException.class) //
				.verify();
	}

	@Test
	public void currentConnectionFactoryShouldReportConnectionFactory() {

		ConnectionFactory factoryMock = mock(ConnectionFactory.class);

		ConnectionFactoryUtils.currentConnectionFactory() //
				.subscriberContext(it -> {
					ReactiveTransactionSynchronization sync = new ReactiveTransactionSynchronization();
					TransactionResources resources = TransactionResources.create();
					resources.registerResource(ConnectionFactory.class, factoryMock);
					sync.registerTransaction(resources);
					return it.put(ReactiveTransactionSynchronization.class, sync);
				}).as(StepVerifier::create) //
				.expectNext(factoryMock) //
				.verifyComplete();
	}
}
