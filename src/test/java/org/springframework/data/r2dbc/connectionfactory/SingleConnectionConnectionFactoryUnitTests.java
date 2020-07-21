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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.h2.H2Connection;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Wrapped;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Test;

/**
 * Unit tests for {@link SingleConnectionConnectionFactory}.
 *
 * @author Mark Paluch
 */
public class SingleConnectionConnectionFactoryUnitTests {

	@Test // gh-204
	public void shouldAllocateSameConnection() {

		SingleConnectionConnectionFactory factory = new SingleConnectionConnectionFactory("r2dbc:h2:mem:///foo", false);

		Mono<? extends Connection> cf1 = factory.create();
		Mono<? extends Connection> cf2 = factory.create();

		Connection c1 = cf1.block();
		Connection c2 = cf2.block();

		assertThat(c1).isSameAs(c2);
		factory.destroy();
	}

	@Test // gh-204
	public void shouldApplyAutoCommit() {

		SingleConnectionConnectionFactory factory = new SingleConnectionConnectionFactory("r2dbc:h2:mem:///foo", false);
		factory.setAutoCommit(false);

		factory.create().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual.isAutoCommit()).isFalse();
		}).verifyComplete();

		factory.setAutoCommit(true);

		factory.create().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual.isAutoCommit()).isTrue();
		}).verifyComplete();

		factory.destroy();
	}

	@Test // gh-204
	public void shouldSuppressClose() {

		SingleConnectionConnectionFactory factory = new SingleConnectionConnectionFactory("r2dbc:h2:mem:///foo", true);

		Connection connection = factory.create().block();

		StepVerifier.create(connection.close()).verifyComplete();
		assertThat(connection).isInstanceOf(Wrapped.class);
		assertThat(((Wrapped) connection).unwrap()).isInstanceOf(H2Connection.class);

		StepVerifier.create(connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED)) //
				.verifyComplete();
		factory.destroy();
	}

	@Test // gh-204
	public void shouldNotSuppressClose() {

		SingleConnectionConnectionFactory factory = new SingleConnectionConnectionFactory("r2dbc:h2:mem:///foo", false);

		Connection connection = factory.create().block();

		StepVerifier.create(connection.close()).verifyComplete();

		StepVerifier.create(connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED))
				.verifyError(R2dbcNonTransientResourceException.class);
		factory.destroy();
	}

	@Test // gh-204
	public void releaseConnectionShouldCloseUnrelatedConnection() {

		Connection connectionMock = mock(Connection.class);
		Connection otherConnection = mock(Connection.class);
		ConnectionFactoryMetadata metadata = mock(ConnectionFactoryMetadata.class);
		when(otherConnection.close()).thenReturn(Mono.empty());

		SingleConnectionConnectionFactory factory = new SingleConnectionConnectionFactory(connectionMock, metadata, false);

		factory.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

		ConnectionFactoryUtils.releaseConnection(otherConnection, factory) //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(otherConnection).close();
	}
}
