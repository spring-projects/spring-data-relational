/*
 * Copyright 2019-2025 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import java.util.List;

/**
 * Factory for common {@link Expression}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see SQL
 * @see Conditions
 * @see Functions
 */
public abstract class Expressions {

	private static Expression ASTERISK = new SimpleExpression("*");

	/**
	 * @return a new asterisk {@code *} expression.
	 */
	public static Expression asterisk() {
		return ASTERISK;
	}

	/**
	 * Creates a plain {@code sql} {@link Expression}.
	 *
	 * @param sql the SQL, must not be {@literal null} or empty.
	 * @return a SQL {@link Expression}.
	 */
	public static Expression just(String sql) {
		return new SimpleExpression(sql);
	}

	/**
	 * @return a new {@link Table}.scoped asterisk {@code <table>.*} expression.
	 */
	public static Expression asterisk(Table table) {
		return table.asterisk();
	}

	/**
	 * @return a new {@link Cast} expression.
	 * @since 2.3
	 */
	public static Expression cast(Expression expression, String targetType) {
		return Cast.create(expression, targetType);
	}

	/**
	 * Creates an {@link Expression} based on the provided list of {@link Column}s.
	 * <p>
	 * If the list contains only a single column, this method returns that column directly as the resulting
	 * {@link Expression}. Otherwise, it creates and returns a {@link TupleExpression} that represents multiple columns as
	 * a single expression.
	 *
	 * @param columns the list of {@link Column}s to include in the expression; must not be {@literal null}.
	 * @return an {@link Expression} corresponding to the input columns: either a single column or a
	 *         {@link TupleExpression} for multiple columns.
	 * @since 4.0
	 */
	public static Expression of(List<Column> columns) {

		if (columns.size() == 1) {
			return columns.get(0);
		}
		return new TupleExpression(columns);
	}

	// Utility constructor.
	private Expressions() {}

	static public class SimpleExpression extends AbstractSegment implements Expression {

		private final String expression;

		SimpleExpression(String expression) {
			this.expression = expression;
		}

		@Override
		public String toString() {
			return expression;
		}
	}
}
