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
import org.springframework.data.relational.core.sql.InlineQuery;
import org.springframework.data.relational.core.sql.Join;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TestJoin;
import org.springframework.data.relational.core.sql.Visitor;

/**
 * Unit tests for {@link JoinVisitor}.
 *
 * @author Jens Schauder
 */
public class JoinVisitorTestsUnitTest {

	final StringBuilder builder = new StringBuilder();
	Visitor visitor = new JoinVisitor(new SimpleRenderContext(NamingStrategies.asIs()), builder::append);

	@ParameterizedTest
	@MethodSource
	void renderJoins(Fixture f) {

		Join join = f.join;

		join.visit(visitor);

		assertThat(builder).hasToString(f.renderResult);
	}

	static List<Fixture> renderJoins() {

		Column colOne = Column.create("colOne", Table.create("tabOne"));
		Table tabTwo = Table.create("tabTwo");
		Column colTwo = Column.create("colTwo", tabTwo);
		Column renamed = colOne.as("renamed");
		Select select = Select.builder().select(renamed).from(colOne.getTable()).build();
		InlineQuery inlineQuery = InlineQuery.create(select, "inline");

		return Arrays.asList(
				fixture("simple join", new TestJoin(Join.JoinType.JOIN, tabTwo, colOne.isEqualTo(colTwo)),
						"JOIN tabTwo ON tabOne.colOne = tabTwo.colTwo"),
				fixture("inlineQuery",
						new TestJoin(Join.JoinType.JOIN, inlineQuery, colTwo.isEqualTo(inlineQuery.column("renamed"))),
						"JOIN (SELECT tabOne.colOne AS renamed FROM tabOne) inline ON tabTwo.colTwo = inline.renamed"));
	}

	private static Fixture fixture(String comment, Join join, String renderResult) {

		Fixture fixture = new Fixture();
		fixture.comment = comment;
		fixture.join = join;
		fixture.renderResult = renderResult;

		return fixture;
	}

	static class Fixture {

		String comment;
		Join join;
		String renderResult;

		@Override
		public String toString() {
			return comment;
		}
	}
}
