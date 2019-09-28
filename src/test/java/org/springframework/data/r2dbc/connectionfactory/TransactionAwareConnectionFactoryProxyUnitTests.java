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
package org.springframework.data.r2dbc.connectionfactory;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Unit tests for {@link TransactionAwareConnectionFactoryProxy}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class TransactionAwareConnectionFactoryProxyUnitTests {

	ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
	Connection connectionMock1 = mock(Connection.class);
	Connection connectionMock2 = mock(Connection.class);
	Connection connectionMock3 = mock(Connection.class);

	private R2dbcTransactionManager tm;

	@Before
	public void before() {

		when(connectionFactoryMock.create()).thenReturn((Mono) Mono.just(connectionMock1),
				(Mono) Mono.just(connectionMock2), (Mono) Mono.just(connectionMock3));
		tm = new R2dbcTransactionManager(connectionFactoryMock);
	}

	@Test // gh-107
	public void createShouldProxyConnection() {

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.as(StepVerifier::create) //
				.consumeNextWith(connection -> {
					assertThat(connection).isInstanceOf(ConnectionProxy.class);
				}).verifyComplete();
	}

	@Test // gh-107
	public void unwrapShouldReturnTargetConnection() {

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThat(proxy.unwrap()).isEqualTo(connectionMock1);
				}).verifyComplete();
	}

	@Test // gh-107
	public void unwrapShouldReturnTargetConnectionEvenWhenClosed() {

		when(connectionMock1.close()).thenReturn(Mono.empty());

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).flatMap(it -> Mono.from(it.close()).then(Mono.just(it)))
				.as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThat(proxy.unwrap()).isEqualTo(connectionMock1);
				}).verifyComplete();
	}

	@Test // gh-107
	public void getTargetConnectionShouldReturnTargetConnection() {

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThat(proxy.getTargetConnection()).isEqualTo(connectionMock1);
				}).verifyComplete();
	}

	@Test // gh-107
	public void getTargetConnectionShouldThrowsErrorEvenWhenClosed() {

		when(connectionMock1.close()).thenReturn(Mono.empty());

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).flatMap(it -> Mono.from(it.close()).then(Mono.just(it)))
				.as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> proxy.getTargetConnection());
				}).verifyComplete();
	}

	@Test // gh-107
	public void hashCodeShouldReturnProxyHash() {

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThat(proxy.hashCode()).isEqualTo(System.identityHashCode(proxy));
				}).verifyComplete();
	}

	@Test // gh-107
	public void equalsShouldCompareCorrectly() {

		new TransactionAwareConnectionFactoryProxy(connectionFactoryMock).create() //
				.map(ConnectionProxy.class::cast).as(StepVerifier::create) //
				.consumeNextWith(proxy -> {
					assertThat(proxy.equals(proxy)).isTrue();
					assertThat(proxy.equals(connectionMock1)).isFalse();
				}).verifyComplete();
	}

	@Test // gh-107
	public void shouldEmitBoundConnection() {

		when(connectionMock1.beginTransaction()).thenReturn(Mono.empty());
		when(connectionMock1.commitTransaction()).thenReturn(Mono.empty());
		when(connectionMock1.close()).thenReturn(Mono.empty());

		TransactionalOperator rxtx = TransactionalOperator.create(tm);
		AtomicReference<Connection> transactionalConnection = new AtomicReference<>();

		TransactionAwareConnectionFactoryProxy proxyCf = new TransactionAwareConnectionFactoryProxy(connectionFactoryMock);

		ConnectionFactoryUtils.getConnection(connectionFactoryMock) //
				.doOnNext(transactionalConnection::set).flatMap(it -> {

					return proxyCf.create().doOnNext(connectionFromProxy -> {

						ConnectionProxy connectionProxy = (ConnectionProxy) connectionFromProxy;
						assertThat(connectionProxy.getTargetConnection()).isSameAs(it);
						assertThat(connectionProxy.unwrap()).isSameAs(it);
					});
				}).as(rxtx::transactional) //
				.flatMapMany(Connection::close) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verifyZeroInteractions(connectionMock2);
		verifyZeroInteractions(connectionMock3);
		verify(connectionFactoryMock, times(1)).create();
	}
}
