/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core;

import lombok.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Builder for creating Select-statements. Not intended for general purpose, but only for the needs of the
 * {@link JdbcAggregateTemplate}.
 *
 * @author Jens Schauder
 */
class SelectBuilder {

	private final List<Column> columns = new ArrayList<>();
	private final String tableName;
	private final List<Join> joins = new ArrayList<>();
	private final List<WhereCondition> conditions = new ArrayList<>();

	/**
	 * Creates a {@link SelectBuilder} using the given table name.
	 * 
	 * @param tableName the table name. Must not be {@code null}.
	 */
	SelectBuilder(String tableName) {

		this.tableName = tableName;
	}

	/**
	 * Adds a column to the select list.
	 *
	 * @param columnSpec a function that specifies the column to add. The passed in {@link Column.ColumnBuilder} allows to
	 *          specify details like alias and the source table. Must not be {@code null}.
	 * @return {@code this}.
	 */
	SelectBuilder column(Function<Column.ColumnBuilder, Column.ColumnBuilder> columnSpec) {

		columns.add(columnSpec.apply(Column.builder()).build());
		return this;
	}

	/**
	 * Adds a where clause to the select
	 *
	 * @param whereSpec a function specifying the details of the where clause by manipulating the passed in
	 *          {@link WhereConditionBuilder}. Must not be {@code null}.
	 * @return {@code this}.
	 */
	SelectBuilder where(Function<WhereConditionBuilder, WhereConditionBuilder> whereSpec) {

		conditions.add(whereSpec.apply(new WhereConditionBuilder()).build());
		return this;
	}

	/**
	 * Adds a join to the select.
	 *
	 * @param joinSpec a function specifying the details of the join by manipulating the passed in
	 *          {@link Join.JoinBuilder}. Must not be {@code null}.
	 * @return {@code this}.
	 */
	SelectBuilder join(Function<Join.JoinBuilder, Join.JoinBuilder> joinSpec) {

		joins.add(joinSpec.apply(Join.builder()).build());
		return this;
	}

	/**
	 * Builds the actual SQL statement.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String build() {

		return selectFrom() + joinClause() + whereClause();
	}

	private String whereClause() {

		if (conditions.isEmpty()) {
			return "";
		}

		return conditions.stream() //
				.map(WhereCondition::toSql) //
				.collect(Collectors.joining("AND", " WHERE ", "") //
		);
	}

	private String joinClause() {
		return joins.stream().map(j -> joinTable(j) + joinConditions(j)).collect(Collectors.joining(" "));
	}

	private String joinTable(Join j) {
		return String.format("%s JOIN %s AS %s", j.outerJoinModifier(), j.table, j.as);
	}

	private String joinConditions(Join j) {

		return j.conditions.stream() //
				.map(w -> String.format("%s %s %s", w.fromExpression, w.operation, w.toExpression)) //
				.collect(Collectors.joining(" AND ", " ON ", ""));
	}

	private String selectFrom() {

		return columns.stream() //
				.map(Column::columnDefinition) //
				.collect(Collectors.joining(", ", "SELECT ", " FROM " + tableName));
	}

	static class WhereConditionBuilder {

		private String fromTable;
		private String fromColumn;

		private String operation = "=";
		private String expression;

		WhereConditionBuilder() {}

		WhereConditionBuilder eq() {

			this.operation = "=";
			return this;
		}

		public WhereConditionBuilder in() {

			this.operation = "in";
			return this;
		}

		WhereConditionBuilder tableAlias(String fromTable) {

			this.fromTable = fromTable;
			return this;
		}

		WhereConditionBuilder column(String fromColumn) {

			this.fromColumn = fromColumn;
			return this;
		}

		WhereConditionBuilder variable(String var) {

			this.expression = ":" + var;
			return this;
		}

		WhereCondition build() {
			return new WhereCondition(fromTable + "." + fromColumn, operation, expression);
		}

	}

	static class Join {

		private final String table;
		private final String as;
		private final Outer outer;
		private final List<WhereCondition> conditions = new ArrayList<>();

		Join(String table, String as, List<WhereCondition> conditions, Outer outer) {

			this.table = table;
			this.as = as;
			this.outer = outer;
			this.conditions.addAll(conditions);
		}

		static JoinBuilder builder() {
			return new JoinBuilder();
		}

		private String outerJoinModifier() {

			switch (outer) {
				case NONE:
					return "";
				default:
					return String.format(" %s OUTER", outer.name());

			}
		}

		public static class JoinBuilder {

			private String table;
			private String as;
			private List<WhereCondition> conditions = new ArrayList<>();
			private Outer outer = Outer.NONE;

			JoinBuilder() {}

			public Join.JoinBuilder table(String table) {

				this.table = table;
				return this;
			}

			public Join.JoinBuilder as(String as) {

				this.as = as;
				return this;
			}

			WhereConditionBuilder where(String column) {
				return new WhereConditionBuilder(this, column);
			}

			private JoinBuilder where(WhereCondition condition) {

				conditions.add(condition);
				return this;
			}

			Join build() {
				return new Join(table, as, conditions, outer);
			}

			public String toString() {
				return "org.springframework.data.jdbc.core.SelectBuilder.Join.JoinBuilder(table=" + this.table + ", as="
						+ this.as + ")";
			}

			JoinBuilder rightOuter() {

				outer = Outer.RIGHT;
				return this;
			}

			JoinBuilder leftOuter() {
				outer = Outer.LEFT;
				return this;
			}

			static class WhereConditionBuilder {

				private final JoinBuilder joinBuilder;
				private final String fromColumn;

				private String operation = "=";

				WhereConditionBuilder(JoinBuilder joinBuilder, String column) {

					this.joinBuilder = joinBuilder;
					this.fromColumn = column;
				}

				WhereConditionBuilder eq() {
					operation = "=";
					return this;
				}

				JoinBuilder column(String table, String column) {

					return joinBuilder.where(new WhereCondition( //
							joinBuilder.as + "." + fromColumn, //
							operation, //
							table + "." + column //
					));

				}

			}

		}

		private enum Outer {
			NONE, RIGHT, LEFT
		}
	}

	static class WhereCondition {

		private final String operation;
		private final String fromExpression;
		private final String toExpression;

		WhereCondition(String fromExpression, String operation, String toExpression) {

			this.fromExpression = fromExpression;
			this.toExpression = toExpression;
			this.operation = operation;
		}

		String toSql() {

			if (operation.equals("in")) {
				return String.format("%s %s(%s)", fromExpression, operation, toExpression);
			}

			return String.format("%s %s %s", fromExpression, operation, toExpression);
		}
	}

	@Builder
	static class Column {

		private final String tableAlias;
		private final String column;
		private final String as;

		String columnDefinition() {
			StringBuilder b = new StringBuilder();
			if (tableAlias != null)
				b.append(tableAlias).append('.');
			b.append(column);
			if (as != null)
				b.append(" AS ").append(as);
			return b.toString();
		}
	}
}
