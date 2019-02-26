package org.springframework.data.r2dbc.function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.function.convert.SettableValue;

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

		assertThat(operation.toQuery()).isEqualTo("INSERT INTO table (firstname, lastname) VALUES($1, $2)");
	}

	@Test // gh-20
	public void shouldRenderUpdateByIdQuery() {

		BindableOperation operation = strategy.updateById("table", new HashSet<>(Arrays.asList("firstname", "lastname")),
				"id");

		assertThat(operation.toQuery()).isEqualTo("UPDATE table SET firstname = $2, lastname = $3 WHERE id = $1");
	}

	@Test // gh-20
	public void shouldRenderDeleteByIdQuery() {

		BindableOperation operation = strategy.deleteById("table", "id");

		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id = $1");
	}

	@Test // gh-20
	public void shouldRenderDeleteByIdInQuery() {

		Statement statement = mock(Statement.class);
		BindIdOperation operation = strategy.deleteByIdIn("table", "id");

		operation.bindId(statement, Collections.singleton("foo"));
		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id IN ($1)");

		operation.bindId(statement, "bar");
		assertThat(operation.toQuery()).isEqualTo("DELETE FROM table WHERE id IN ($1, $2)");
	}

	@Test // gh-22
	public void shouldUpdateArray() {

		Map<String, SettableValue> columnsToUpdate = strategy
				.getColumnsToUpdate(new WithCollectionTypes(new String[] { "one", "two" }, null));

		Object stringArray = columnsToUpdate.get("string_array").getValue();

		assertThat(stringArray).isInstanceOf(String[].class);
		assertThat((String[]) stringArray).hasSize(2).contains("one", "two");
	}

	@Test // gh-22
	public void shouldConvertListToArray() {

		Map<String, SettableValue> columnsToUpdate = strategy
				.getColumnsToUpdate(new WithCollectionTypes(null, Arrays.asList("one", "two")));

		Object stringArray = columnsToUpdate.get("string_collection").getValue();

		assertThat(stringArray).isInstanceOf(String[].class);
		assertThat((String[]) stringArray).hasSize(2).contains("one", "two");
	}

	static class WithCollectionTypes {

		String[] stringArray;

		List<String> stringCollection;

		WithCollectionTypes(String[] stringArray, List<String> stringCollection) {

			this.stringArray = stringArray;
			this.stringCollection = stringCollection;
		}
	}
}
