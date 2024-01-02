/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.InlineQuery;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TestFrom;

/**
 * Unit tests for the {@link FromClauseVisitor}.
 *
 * @author Jens Schauder
 */
class FromClauseVisitorUnitTests {

	StringBuilder renderResult = new StringBuilder();
	FromClauseVisitor visitor = new FromClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()), renderResult::append);

	@ParameterizedTest
	@MethodSource
	void testRendering(Fixture f) {

		From from = f.from;

		from.visit(visitor);

		assertThat(renderResult).hasToString(f.renderResult);
	}

	static List<Fixture> testRendering() {

		Table tabOne = Table.create("tabOne");
		Table tabTwo = Table.create("tabTwo");
		Select selectOne = Select.builder().select(Column.create("oneId", tabOne)).from(tabOne).build();
		Select selectTwo = Select.builder().select(Column.create("twoId", tabTwo)).from(tabTwo).build();

		return Arrays.asList(
				fixture("single table", new TestFrom(Table.create("one")), "one"),
				fixture("single table with alias", new TestFrom(Table.aliased("one", "one_alias")), "one one_alias"),
				fixture("multiple tables", new TestFrom(Table.create("one"),Table.create("two")), "one, two"),
				fixture("multiple tables with alias", new TestFrom(Table.aliased("one", "one_alias"),Table.aliased("two", "two_alias")), "one one_alias, two two_alias"),
				fixture("single inline query", new TestFrom(InlineQuery.create(selectOne, "ilAlias")), "(SELECT tabOne.oneId FROM tabOne) ilAlias"),
				fixture("inline query with table", new TestFrom(InlineQuery.create(selectOne, "ilAlias"), tabTwo), "(SELECT tabOne.oneId FROM tabOne) ilAlias, tabTwo"),
				fixture("table with inline query", new TestFrom(tabTwo,InlineQuery.create(selectOne, "ilAlias")), "tabTwo, (SELECT tabOne.oneId FROM tabOne) ilAlias"),
				fixture("two inline queries", new TestFrom(InlineQuery.create(selectOne, "aliasOne"),InlineQuery.create(selectTwo, "aliasTwo")), "(SELECT tabOne.oneId FROM tabOne) aliasOne, (SELECT tabTwo.twoId FROM tabTwo) aliasTwo")
		);
	}

	private static Fixture fixture(String comment, From from, String renderResult) {

		Fixture fixture = new Fixture();
		fixture.comment = comment;
		fixture.from = from;
		fixture.renderResult = renderResult;
		return fixture;
	}

	static class Fixture {

		String comment;
		From from;
		String renderResult;

		@Override
		public String toString() {
			return comment;
		}
	}
}
