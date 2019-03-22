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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Publisher;

/**
 * Connection holder, wrapping a R2DBC Connection.
 * {@link org.springframework.data.r2dbc.function.TransactionalDatabaseClient} binds instances of this class to the
 * {@link TransactionResources} for a specific subscription.
 *
 * @author Mark Paluch
 */
class SingletonConnectionFactory implements SmartConnectionFactory {

	private final ConnectionFactoryMetadata metadata;
	private final Connection connection;
	private final Mono<Connection> connectionMono;
	private final AtomicInteger refCount = new AtomicInteger();

	SingletonConnectionFactory(ConnectionFactoryMetadata metadata, Connection connection) {

		this.metadata = metadata;
		this.connection = connection;
		this.connectionMono = Mono.just(connection);
	}

	/* (non-Javadoc)
	 * @see io.r2dbc.spi.ConnectionFactory#create()
	 */
	@Override
	public Publisher<? extends Connection> create() {

		if (refCount.get() == -1) {
			throw new IllegalStateException("Connection is closed!");
		}

		return connectionMono.doOnSubscribe(s -> refCount.incrementAndGet());
	}

	/* (non-Javadoc)
	 * @see io.r2dbc.spi.ConnectionFactory#getMetadata()
	 */
	@Override
	public ConnectionFactoryMetadata getMetadata() {
		return metadata;
	}

	private boolean connectionEquals(Connection connection) {
		return this.connection == connection;
	}

	@Override
	public boolean shouldClose(Connection connection) {
		return refCount.get() == 1;
	}

	Mono<Void> close(Connection connection) {

		if (connectionEquals(connection)) {
			return Mono.<Void> empty().doOnSubscribe(s -> refCount.decrementAndGet());
		}

		throw new IllegalArgumentException("Connection is not associated with this connection factory");
	}
}
