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
package org.springframework.data.r2dbc.core;

import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import org.springframework.data.r2dbc.dialect.PostgresDialect;
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
	@Mock R2dbcExceptionTranslator translator;

	@Before
	public void before() {
		when(connectionFactory.create()).thenReturn((Publisher) Mono.just(connection));
		when(connection.close()).thenReturn(Mono.empty());
	}

	@Test // gh-48
	public void shouldCloseConnectionOnlyOnce() {

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE))
				.exceptionTranslator(translator).build();

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

	@Test // gh-128
	public void executeShouldBindNullValues() {

		Statement statement = mock(Statement.class);
		when(connection.createStatement("SELECT * FROM table WHERE key = $1")).thenReturn(statement);
		when(statement.execute()).thenReturn(Mono.empty());

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bindNull(0, String.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull(0, String.class);

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bindNull("$1", String.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull("$1", String.class);
	}

	@Test // gh-128
	public void executeShouldBindNamedNullValues() {

		Statement statement = mock(Statement.class);
		when(connection.createStatement("SELECT * FROM table WHERE key = $1")).thenReturn(statement);
		when(statement.execute()).thenReturn(Mono.empty());

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();

		databaseClient.execute("SELECT * FROM table WHERE key = :key") //
				.bindNull("key", String.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull(0, String.class);
	}

	@Test // gh-128
	public void executeShouldBindValues() {

		Statement statement = mock(Statement.class);
		when(connection.createStatement("SELECT * FROM table WHERE key = $1")).thenReturn(statement);
		when(statement.execute()).thenReturn(Mono.empty());

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bind(0, "foo") //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bind("$1", "foo") //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind("$1", "foo");
	}

	@Test // gh-128
	public void executeShouldBindNamedValuesByIndex() {

		Statement statement = mock(Statement.class);
		when(connection.createStatement("SELECT * FROM table WHERE key = $1")).thenReturn(statement);
		when(statement.execute()).thenReturn(Mono.empty());

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) DatabaseClient.builder()
				.connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();

		databaseClient.execute("SELECT * FROM table WHERE key = :key") //
				.bind("key", "foo") //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");
	}
}
