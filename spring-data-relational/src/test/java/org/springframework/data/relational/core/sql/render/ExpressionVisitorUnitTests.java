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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.Table;

/**
 * Tests for the {@link ExpressionVisitor}.
 *
 * @author Jens Schauder
 */
public class ExpressionVisitorUnitTests {

	static SimpleRenderContext simpleRenderContext = new SimpleRenderContext(NamingStrategies.asIs());

	@ParameterizedTest // GH-1003
	@MethodSource
	void expressionsWithOutAliasGetRendered(Fixture f) {

		ExpressionVisitor visitor = new ExpressionVisitor(simpleRenderContext);

		f.expression.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).as(f.comment).isEqualTo(f.renderResult);
	}

	static List<Fixture> expressionsWithOutAliasGetRendered() {

		// final Select select = Select.builder().select(Functions.count(Expressions.asterisk()),
		// SQL.nullLiteral()).build();

		return asList( //
				fixture("String literal", SQL.literalOf("one"), "'one'"), //
				fixture("Numeric literal", SQL.literalOf(23L), "23"), //
				fixture("Boolean literal", SQL.literalOf(true), "TRUE"), //
				fixture("Just", SQL.literalOf(Expressions.just("just an arbitrary String")), "just an arbitrary String"), //
				fixture("Column", Column.create("col", Table.create("tab")), "tab.col"), //
				fixture("*", Expressions.asterisk(), "*"), //
				fixture("tab.*", Expressions.asterisk(Table.create("tab")), "tab.*"), //
				fixture("Count 1", Functions.count(SQL.literalOf(1)), "COUNT(1)"), //
				fixture("Count *", Functions.count(Expressions.asterisk()), "COUNT(*)"), //
				fixture("Function", SimpleFunction.create("Function", asList(SQL.literalOf("one"), SQL.literalOf("two"))), //
						"Function('one', 'two')"), //
				fixture("Null", SQL.nullLiteral(), "NULL"), //
				fixture("Cast", Expressions.cast(Column.create("col", Table.create("tab")), "JSON"), "CAST(tab.col AS JSON)"), //
				fixture("Cast with alias", Expressions.cast(Column.create("col", Table.create("tab")).as("alias"), "JSON"),
						"CAST(tab.col AS JSON)")); //
	}

	@Test // GH-1003
	void renderAliasedExpressionWithAliasHandlingUse() {

		ExpressionVisitor visitor = new ExpressionVisitor(simpleRenderContext, ExpressionVisitor.AliasHandling.USE);

		Column expression = Column.aliased("col", Table.create("tab"), "col_alias");
		expression.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("col_alias");
	}

	@Test // GH-1003
	void renderAliasedExpressionWithAliasHandlingDeclare() {

		ExpressionVisitor visitor = new ExpressionVisitor(simpleRenderContext, ExpressionVisitor.AliasHandling.IGNORE);

		Column expression = Column.aliased("col", Table.create("tab"), "col_alias");
		expression.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("tab.col");
	}

	@Test // GH-1003
	void considersNamingStrategy() {

		ExpressionVisitor visitor = new ExpressionVisitor(new SimpleRenderContext(NamingStrategies.toUpper()));

		Column expression = Column.create("col", Table.create("tab"));
		expression.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("TAB.COL");
	}

	@Test // GH-1003
	void considerNamingStrategyForTableAsterisk() {

		ExpressionVisitor visitor = new ExpressionVisitor(new SimpleRenderContext(NamingStrategies.toUpper()));

		Expression expression = Table.create("tab").asterisk();
		expression.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("TAB.*");
	}

	static Fixture fixture(String comment, Expression expression, String renderResult) {

		Fixture f = new Fixture();
		f.comment = comment;
		f.expression = expression;
		f.renderResult = renderResult;

		return f;
	}

	static class Fixture {

		String comment;
		Expression expression;
		String renderResult;

		@Override
		public String toString() {
			return comment;
		}
	}
}
