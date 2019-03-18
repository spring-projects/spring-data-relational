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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Unit tests for {@link TransactionAwareConnectionFactoryProxy}.
 *
 * @author Mark Paluch
 */
public class TransactionAwareConnectionFactoryProxyUnitTests {

	ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
	Connection connectionMock1 = mock(Connection.class);
	Connection connectionMock2 = mock(Connection.class);
	Connection connectionMock3 = mock(Connection.class);

	private ConnectionFactoryTransactionManager tm;

	@Before
	public void before() {

		when(connectionFactoryMock.create()).thenReturn((Mono) Mono.just(connectionMock1),
				(Mono) Mono.just(connectionMock2), (Mono) Mono.just(connectionMock3));
		tm = new ConnectionFactoryTransactionManager(connectionFactoryMock);
	}

	@Test // gh-107
	public void shouldEmitBoundConnection() {

		when(connectionMock1.beginTransaction()).thenReturn(Mono.empty());
		when(connectionMock1.commitTransaction()).thenReturn(Mono.error(new IllegalStateException()));
		when(connectionMock1.close()).thenReturn(Mono.empty());

		TransactionalOperator rxtx = TransactionalOperator.create(tm);
		AtomicReference<Connection> transactionalConnection = new AtomicReference<>();

		TransactionAwareConnectionFactoryProxy proxyCf = new TransactionAwareConnectionFactoryProxy(connectionFactoryMock);

		ConnectionFactoryUtils.getConnection(connectionFactoryMock).map(Tuple2::getT1) //
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
