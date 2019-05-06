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
package org.springframework.data.r2dbc.function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.domain.PreparedOperation;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Update;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 */
public class StatementFactoryUnitTests {

	// See https://github.com/spring-projects/spring-data-r2dbc/issues/55
	DefaultStatementFactory statements = new DefaultStatementFactory(PostgresDialect.INSTANCE,
			new RenderContextFactory(org.springframework.data.relational.core.dialect.PostgresDialect.INSTANCE)
					.createRenderContext());

	Statement statementMock = mock(Statement.class);
	Connection connectionMock = mock(Connection.class);

	@Before
	public void before() {
		when(connectionMock.createStatement(anyString())).thenReturn(statementMock);
	}

	@Test
	public void shouldToQuerySimpleSelectWithoutBindings() {

		PreparedOperation<Select> select = statements.select("foo", Arrays.asList("bar", "baz"), it -> {});

		assertThat(select.getSource()).isInstanceOf(Select.class);
		assertThat(select.toQuery()).isEqualTo("SELECT foo.bar, foo.baz FROM foo");

		createBoundStatement(select, connectionMock);

		verifyZeroInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleSelectWithSimpleFilter() {

		PreparedOperation<Select> select = statements.select("foo", Arrays.asList("bar", "baz"), it -> {
			it.filterBy("doe", SettableValue.from("John"));
		});

		assertThat(select.getSource()).isInstanceOf(Select.class);
		assertThat(select.toQuery()).isEqualTo("SELECT foo.bar, foo.baz FROM foo WHERE foo.doe = $1");

		createBoundStatement(select, connectionMock);

		verify(statementMock).bind(0, "John");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleSelectWithMultipleFilters() {

		PreparedOperation<Select> select = statements.select("foo", Arrays.asList("bar", "baz"), it -> {
			it.filterBy("doe", SettableValue.from("John"));
			it.filterBy("baz", SettableValue.from("Jake"));
		});

		assertThat(select.getSource()).isInstanceOf(Select.class);
		assertThat(select.toQuery()).isEqualTo("SELECT foo.bar, foo.baz FROM foo WHERE foo.doe = $1 AND foo.baz = $2");

		createBoundStatement(select, connectionMock);

		verify(statementMock).bind(0, "John");
		verify(statementMock).bind(1, "Jake");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleSelectWithNullFilter() {

		PreparedOperation<Select> select = statements.select("foo", Arrays.asList("bar", "baz"), it -> {
			it.filterBy("doe", SettableValue.empty(String.class));
		});

		assertThat(select.getSource()).isInstanceOf(Select.class);
		assertThat(select.toQuery()).isEqualTo("SELECT foo.bar, foo.baz FROM foo WHERE foo.doe IS NULL");

		createBoundStatement(select, connectionMock);
		verifyZeroInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleSelectWithIterableFilter() {

		PreparedOperation<Select> select = statements.select("foo", Arrays.asList("bar", "baz"), it -> {
			it.filterBy("doe", SettableValue.from(Arrays.asList("John", "Jake")));
		});

		assertThat(select.getSource()).isInstanceOf(Select.class);
		assertThat(select.toQuery()).isEqualTo("SELECT foo.bar, foo.baz FROM foo WHERE foo.doe IN ($1, $2)");

		createBoundStatement(select, connectionMock);
		verify(statementMock).bind(0, "John");
		verify(statementMock).bind(1, "Jake");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldFailInsertToQueryingWithoutValueBindings() {

		assertThatThrownBy(() -> statements.insert("foo", Collections.emptyList(), it -> {}))
				.isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void shouldToQuerySimpleInsert() {

		PreparedOperation<Insert> insert = statements.insert("foo", Collections.emptyList(), it -> {
			it.bind("bar", SettableValue.from("Foo"));
		});

		assertThat(insert.getSource()).isInstanceOf(Insert.class);
		assertThat(insert.toQuery()).isEqualTo("INSERT INTO foo (bar) VALUES ($1)");

		createBoundStatement(insert, connectionMock);
		verify(statementMock).bind(0, "Foo");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldFailUpdateToQueryingWithoutValueBindings() {

		assertThatThrownBy(() -> statements.update("foo", it -> it.filterBy("foo", SettableValue.empty(Object.class))))
				.isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void shouldToQuerySimpleUpdate() {

		PreparedOperation<Update> update = statements.update("foo", it -> {
			it.bind("bar", SettableValue.from("Foo"));
		});

		assertThat(update.getSource()).isInstanceOf(Update.class);
		assertThat(update.toQuery()).isEqualTo("UPDATE foo SET bar = $1");

		createBoundStatement(update, connectionMock);
		verify(statementMock).bind(0, "Foo");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQueryNullUpdate() {

		PreparedOperation<Update> update = statements.update("foo", it -> {
			it.bind("bar", SettableValue.empty(String.class));
		});

		assertThat(update.getSource()).isInstanceOf(Update.class);
		assertThat(update.toQuery()).isEqualTo("UPDATE foo SET bar = $1");

		createBoundStatement(update, connectionMock);
		verify(statementMock).bindNull(0, String.class);

		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQueryUpdateWithFilter() {

		PreparedOperation<Update> update = statements.update("foo", it -> {
			it.bind("bar", SettableValue.from("Foo"));
			it.filterBy("baz", SettableValue.from("Baz"));
		});

		assertThat(update.getSource()).isInstanceOf(Update.class);
		assertThat(update.toQuery()).isEqualTo("UPDATE foo SET bar = $1 WHERE foo.baz = $2");

		createBoundStatement(update, connectionMock);
		verify(statementMock).bind(0, "Foo");
		verify(statementMock).bind(1, "Baz");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleDeleteWithSimpleFilter() {

		PreparedOperation<Delete> delete = statements.delete("foo", it -> {
			it.filterBy("doe", SettableValue.from("John"));
		});

		assertThat(delete.getSource()).isInstanceOf(Delete.class);
		assertThat(delete.toQuery()).isEqualTo("DELETE FROM foo WHERE foo.doe = $1");

		createBoundStatement(delete, connectionMock);
		verify(statementMock).bind(0, "John");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleDeleteWithMultipleFilters() {

		PreparedOperation<Delete> delete = statements.delete("foo", it -> {
			it.filterBy("doe", SettableValue.from("John"));
			it.filterBy("baz", SettableValue.from("Jake"));
		});

		assertThat(delete.getSource()).isInstanceOf(Delete.class);
		assertThat(delete.toQuery()).isEqualTo("DELETE FROM foo WHERE foo.doe = $1 AND foo.baz = $2");

		createBoundStatement(delete, connectionMock);
		verify(statementMock).bind(0, "John");
		verify(statementMock).bind(1, "Jake");
		verifyNoMoreInteractions(statementMock);
	}

	@Test
	public void shouldToQuerySimpleDeleteWithNullFilter() {

		PreparedOperation<Delete> delete = statements.delete("foo", it -> {
			it.filterBy("doe", SettableValue.empty(String.class));
		});

		assertThat(delete.getSource()).isInstanceOf(Delete.class);
		assertThat(delete.toQuery()).isEqualTo("DELETE FROM foo WHERE foo.doe IS NULL");

		createBoundStatement(delete, connectionMock);
		verifyZeroInteractions(statementMock);
	}

	void createBoundStatement(PreparedOperation<?> operation, Connection connection) {

		Statement statement = connection.createStatement(operation.toQuery());
		operation.bindTo(new DefaultDatabaseClient.StatementWrapper(statement));
	}
}
