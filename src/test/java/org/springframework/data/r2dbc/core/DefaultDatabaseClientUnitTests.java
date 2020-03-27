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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.r2dbc.query.Criteria.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link DefaultDatabaseClient}.
 *
 * @author Mark Paluch
 * @author Ferdinand Jacobs
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultDatabaseClientUnitTests {

	@Mock Connection connection;
	private DatabaseClient.Builder databaseClientBuilder;

	@Before
	public void before() {

		ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);

		when(connectionFactory.create()).thenReturn((Publisher) Mono.just(connection));
		when(connection.close()).thenReturn(Mono.empty());

		databaseClientBuilder = DatabaseClient.builder() //
				.connectionFactory(connectionFactory) //
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));
	}

	@Test // gh-48
	public void shouldCloseConnectionOnlyOnce() {

		DefaultDatabaseClient databaseClient = (DefaultDatabaseClient) databaseClientBuilder.build();

		Flux<Object> flux = databaseClient.inConnectionMany(it -> Flux.empty());

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

		Statement statement = mockStatementFor("SELECT * FROM table WHERE key = $1");

		DatabaseClient databaseClient = databaseClientBuilder.build();

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

	@Test // gh-162
	public void executeShouldBindSettableValues() {

		Statement statement = mockStatementFor("SELECT * FROM table WHERE key = $1");

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bind(0, SettableValue.empty(String.class)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull(0, String.class);

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bind("$1", SettableValue.empty(String.class)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull("$1", String.class);
	}

	@Test // gh-128
	public void executeShouldBindNamedNullValues() {

		Statement statement = mockStatementFor("SELECT * FROM table WHERE key = $1");
		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT * FROM table WHERE key = :key") //
				.bindNull("key", String.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bindNull(0, String.class);
	}

	@Test // gh-178
	public void executeShouldBindNamedValuesFromIndexes() {

		Statement statement = mockStatementFor("SELECT id, name, manual FROM legoset WHERE name IN ($1, $2, $3)");

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT id, name, manual FROM legoset WHERE name IN (:name)") //
				.bind(0, Arrays.asList("unknown", "dunno", "other")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "unknown");
		verify(statement).bind(1, "dunno");
		verify(statement).bind(2, "other");
		verify(statement).execute();
		verifyNoMoreInteractions(statement);
	}

	@Test // gh-128, gh-162
	public void executeShouldBindValues() {

		Statement statement = mockStatementFor("SELECT * FROM table WHERE key = $1");

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT * FROM table WHERE key = $1") //
				.bind(0, SettableValue.from("foo")) //
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

	@Test // gh-162
	public void insertShouldAcceptNullValues() {

		Statement statement = mockStatementFor("INSERT INTO foo (first, second) VALUES ($1, $2)");
		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.insert().into("foo") //
				.value("first", "foo") //
				.nullValue("second", Integer.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");
		verify(statement).bindNull(1, Integer.class);
	}

	@Test // gh-162
	public void insertShouldAcceptSettableValue() {

		Statement statement = mockStatementFor("INSERT INTO foo (first, second) VALUES ($1, $2)");
		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.insert().into("foo") //
				.value("first", SettableValue.from("foo")) //
				.value("second", SettableValue.empty(Integer.class)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");
		verify(statement).bindNull(1, Integer.class);
	}

	@Test // gh-128
	public void executeShouldBindNamedValuesByIndex() {

		Statement statement = mockStatementFor("SELECT * FROM table WHERE key = $1");
		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT * FROM table WHERE key = :key") //
				.bind("key", "foo") //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");
	}

	@Test // gh-177
	public void deleteNotInShouldRenderCorrectQuery() {

		Statement statement = mockStatementFor("DELETE FROM tab WHERE tab.pole = $1 AND tab.id NOT IN ($2, $3)");
		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.delete().from("tab").matching(where("pole").is("foo").and("id").notIn(1, 2)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		verify(statement).bind(0, "foo");
		verify(statement).bind(1, 1);
		verify(statement).bind(2, 2);
	}

	@Test // gh-243
	public void rowsUpdatedShouldEmitSingleValue() {

		Result result = mock(Result.class);
		when(result.getRowsUpdated()).thenReturn(Mono.empty(), Mono.just(2), Flux.just(1, 2, 3));
		mockStatementFor("DROP TABLE tab;", result);

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("DROP TABLE tab;") //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		databaseClient.execute("DROP TABLE tab;") //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		databaseClient.execute("DROP TABLE tab;") //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // gh-250
	public void shouldThrowExceptionForSingleColumnObjectUpdate() {

		DatabaseClient databaseClient = databaseClientBuilder.build();

		assertThatIllegalArgumentException().isThrownBy(() -> databaseClient.update() //
				.table(IdOnly.class) //
				.using(new IdOnly()) //
				.then()).withMessageContaining("UPDATE contains no assignments");
	}

	@Test // gh-260
	public void shouldProjectGenericExecuteAs() {

		MockResult result = mockSingleColumnResult(MockRow.builder().identified(0, Object.class, "Walter"));
		mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder //
				.projectionFactory(new SpelAwareProxyProjectionFactory()) //
				.build();

		databaseClient.execute("SELECT * FROM person") //
				.as(Projection.class) //
				.fetch() //
				.one() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getName()).isEqualTo("Walter");
					assertThat(actual.getGreeting()).isEqualTo("Hello Walter");

				}) //
				.verifyComplete();
	}

	@Test // gh-260
	public void shouldProjectGenericSelectAs() {

		MockResult result = mockSingleColumnResult(MockRow.builder().identified(0, Object.class, "Walter"));
		mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder //
				.projectionFactory(new SpelAwareProxyProjectionFactory()) //
				.build();

		databaseClient.select().from("person") //
				.project("*") //
				.as(Projection.class) //
				.fetch() //
				.one() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getName()).isEqualTo("Walter");
					assertThat(actual.getGreeting()).isEqualTo("Hello Walter");

				}) //
				.verifyComplete();
	}

	@Test // gh-260
	public void shouldProjectTypedSelectAs() {

		MockResult result = mockSingleColumnResult(MockRow.builder().identified("name", Object.class, "Walter"));
		mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder //
				.projectionFactory(new SpelAwareProxyProjectionFactory()) //
				.build();

		databaseClient.select().from(Person.class) //
				.as(Projection.class) //
				.one() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getName()).isEqualTo("Walter");
					assertThat(actual.getGreeting()).isEqualTo("Hello Walter");

				}) //
				.verifyComplete();
	}

	@Test // gh-189
	public void shouldApplyExecuteFunction() {

		Statement statement = mockStatement();
		MockResult result = mockSingleColumnResult(MockRow.builder().identified(0, Object.class, "Walter"));

		DatabaseClient databaseClient = databaseClientBuilder //
				.executeFunction(it -> Mono.just(result)) //
				.build();

		databaseClient.execute("SELECT") //
				.fetch().all() //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		verifyNoInteractions(statement);
	}

	@Test // gh-189
	public void shouldApplyStatementFilterFunctions() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata).build();

		Statement statement = mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT") //
				.filter((s, next) -> next.execute(s.returnGeneratedValues("foo"))) //
				.filter((s, next) -> next.execute(s.returnGeneratedValues("bar"))) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.verifyComplete();

		InOrder inOrder = inOrder(statement);
		inOrder.verify(statement).returnGeneratedValues("foo");
		inOrder.verify(statement).returnGeneratedValues("bar");
		inOrder.verify(statement).execute();
		inOrder.verifyNoMoreInteractions();
	}

	@Test // gh-189
	public void shouldApplyStatementFilterFunctionsToTypedExecute() {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();
		MockResult result = MockResult.builder().rowMetadata(metadata).build();

		Statement statement = mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT") //
				.filter((s, next) -> next.execute(s.returnGeneratedValues("foo"))) //
				.as(Person.class) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.verifyComplete();

		InOrder inOrder = inOrder(statement);
		inOrder.verify(statement).returnGeneratedValues("foo");
		inOrder.verify(statement).execute();
		inOrder.verifyNoMoreInteractions();
	}

	@Test // gh-189
	public void shouldApplySimpleStatementFilterFunctions() {

		MockResult result = mockSingleColumnEmptyResult();

		Statement statement = mockStatement(result);

		DatabaseClient databaseClient = databaseClientBuilder.build();

		databaseClient.execute("SELECT") //
				.filter(s -> s.returnGeneratedValues("foo")) //
				.filter(s -> s.returnGeneratedValues("bar")) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.verifyComplete();

		InOrder inOrder = inOrder(statement);
		inOrder.verify(statement).returnGeneratedValues("foo");
		inOrder.verify(statement).returnGeneratedValues("bar");
		inOrder.verify(statement).execute();
		inOrder.verifyNoMoreInteractions();
	}

	private Statement mockStatement() {
		return mockStatementFor(null, null);
	}

	private Statement mockStatement(Result result) {
		return mockStatementFor(null, result);
	}

	private Statement mockStatementFor(String sql) {
		return mockStatementFor(sql, null);
	}

	private Statement mockStatementFor(@Nullable String sql, @Nullable Result result) {

		Statement statement = mock(Statement.class);
		when(connection.createStatement(sql == null ? anyString() : eq(sql))).thenReturn(statement);
		when(statement.returnGeneratedValues(anyString())).thenReturn(statement);
		when(statement.returnGeneratedValues()).thenReturn(statement);

		doReturn(result == null ? Mono.empty() : Flux.just(result)).when(statement).execute();

		return statement;
	}

	private MockResult mockSingleColumnEmptyResult() {
		return mockSingleColumnResult(null);
	}

	/**
	 * Mocks a {@link Result} with a single column "name" and a single row if a non null row is provided.
	 */
	private MockResult mockSingleColumnResult(@Nullable MockRow.Builder row) {

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("name").build()).build();

		MockResult.Builder resultBuilder = MockResult.builder().rowMetadata(metadata);
		if (row != null) {
			resultBuilder = resultBuilder.row(row.build());
		}
		return resultBuilder.build();
	}

	static class Person {

		String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	interface Projection {

		String getName();

		@Value("#{'Hello ' + target.name}")
		String getGreeting();
	}

	static class IdOnly {

		@Id String id;
	}
}
