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
package org.springframework.data.r2dbc.connectionfactory;

import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.connectionfactory.ConnectionFactoryUtils;
import org.springframework.data.r2dbc.connectionfactory.ReactiveTransactionSynchronization;
import org.springframework.data.r2dbc.connectionfactory.TransactionResources;
import org.springframework.transaction.NoTransactionException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link ConnectionFactoryUtils}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class ConnectionFactoryUtilsUnitTests {

	@Test // gh-107
	public void currentReactiveTransactionSynchronizationShouldReportSynchronization() {

		ConnectionFactoryUtils.currentReactiveTransactionSynchronization() //
				.subscriberContext(
						it -> it.put(ReactiveTransactionSynchronization.class, new ReactiveTransactionSynchronization()))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // gh-107
	public void currentReactiveTransactionSynchronizationShouldFailWithoutTxMgmt() {

		ConnectionFactoryUtils.currentReactiveTransactionSynchronization() //
				.as(StepVerifier::create) //
				.expectError(NoTransactionException.class) //
				.verify();
	}

	@Test // gh-107
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

	@Test // gh-107
	public void currentActiveReactiveTransactionSynchronization() {

		ConnectionFactoryUtils.currentActiveReactiveTransactionSynchronization() //
				.subscriberContext(
						it -> it.put(ReactiveTransactionSynchronization.class, new ReactiveTransactionSynchronization()))
				.as(StepVerifier::create) //
				.expectError(NoTransactionException.class) //
				.verify();
	}

	@Test // gh-107
	public void currentConnectionFactoryShouldReportConnectionFactory() {

		ConnectionFactory factoryMock = mock(ConnectionFactory.class);

		ConnectionFactoryUtils.currentConnectionFactory(factoryMock) //
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

	@Test // gh-107
	public void connectionFactoryRetunsConnectionWhenNoSyncronisationActive() {

		ConnectionFactory factoryMock = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Publisher<? extends Connection> p = Mono.just(connection);
		doReturn(p).when(factoryMock).create();

		ConnectionFactoryUtils.getConnection(factoryMock) //
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {
					Assertions.assertThat(it.getT1()).isEqualTo(connection);
					Assertions.assertThat(it.getT2()).isEqualTo(factoryMock);
				})
				.verifyComplete();
	}
}
