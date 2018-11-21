package org.springframework.data.r2dbc.function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;
import org.springframework.data.r2dbc.dialect.PostgresDialect;

/**
 * Unit tests for {@link DefaultReactiveDataAccessStrategy}.
 *
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategyUnitTests {

	DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE);

	@Test // gh-20
	public void shouldRenderInsertAndReturnGeneratedKeysQuery() {

		BindableOperation operation = strategy.insertAndReturnGeneratedKeys("table",
				new HashSet<>(Arrays.asList("firstname", "lastname")));

		assertThat(operation.toQuery()).isEqualTo("INSERT INTO table (firstname, lastname) VALUES($1, $2) RETURNING *");
	}

	@Test // gh-20
	public void shouldRenderUpdateByIdQuery() {

		BindableOperation operation = strategy.updateById("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id");

		assertThat(operation.toQuery()).isEqualTo("UPDATE table SET firstname = $2, lastname = $3 WHERE id = $1");
	}

	@Test // gh-20
	public void shouldRenderSelectByIdQuery() {

		BindableOperation operation = strategy.selectById("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id");

		assertThat(operation.toQuery()).isEqualTo("SELECT firstname, lastname FROM table WHERE id = $1");
	}

	@Test // gh-20
	public void shouldRenderSelectByIdQueryWithLimit() {

		BindableOperation operation = strategy.selectById("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id", 10);

		assertThat(operation.toQuery())
				.isEqualTo("SELECT firstname, lastname FROM table WHERE id = $1 ORDER BY id LIMIT 10");
	}

	@Test // gh-20
	public void shouldFailRenderingSelectByIdInQueryWithoutBindings() {

		BindableOperation operation = strategy.selectByIdIn("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id");

		assertThatThrownBy(operation::toQuery).isInstanceOf(UnsupportedOperationException.class);
	}

	@Test // gh-20
	public void shouldRenderSelectByIdInQuery() {

		Statement<?> statement = mock(Statement.class);
		BindIdOperation operation = strategy.selectByIdIn("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id");

		operation.bindId(statement, Collections.singleton("foo"));
		assertThat(operation.toQuery()).isEqualTo("SELECT firstname, lastname FROM table WHERE id IN ($1)");

		operation.bindId(statement, "bar");
		assertThat(operation.toQuery()).isEqualTo("SELECT firstname, lastname FROM table WHERE id IN ($1, $2)");
	}

	@Test // gh-20
	public void shouldRenderDeleteByIdQuery() {

		BindableOperation operation = strategy.deleteById("table", "id");

		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id = $1");
	}

	@Test // gh-20
	public void shouldRenderDeleteByIdInQuery() {

		Statement<?> statement = mock(Statement.class);
		BindIdOperation operation = strategy.deleteByIdIn("table", "id");

		operation.bindId(statement, Collections.singleton("foo"));
		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id IN ($1)");

		operation.bindId(statement, "bar");
		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id IN ($1, $2)");
	}
}
