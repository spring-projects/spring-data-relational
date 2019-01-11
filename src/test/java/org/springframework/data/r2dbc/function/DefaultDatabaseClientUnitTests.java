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
package org.springframework.data.r2dbc.function;

import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;

/**
 * Unit tests for {@link DefaultDatabaseClient}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultDatabaseClientUnitTests {

	@Mock ConnectionFactory connectionFactory;
	@Mock Connection connection;
	@Mock ReactiveDataAccessStrategy strategy;
	@Mock R2dbcExceptionTranslator translator;

	@Before
	public void before() {
		when(connectionFactory.create()).thenReturn((Publisher) Mono.just(connection));
		when(connection.close()).thenReturn(Mono.empty());
	}

	@Test // gh-48
	public void shouldCloseConnectionOnlyOnce() {

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory).dataAccessStrategy(strategy).exceptionTranslator(translator).build();

		Flux<Object> flux = databaseClient.inConnectionMany(it -> {
			return Flux.empty();
		});

		flux.subscribe(new CoreSubscriber<Object>() {
			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				s.request(1);
				subscription = s;
			}

			@Override
			public void onNext(Object o) {}

			@Override
			public void onError(Throwable t) {}

			@Override
			public void onComplete() {
				subscription.cancel();
			}
		});

		verify(connection, times(1)).close();
	}
}
