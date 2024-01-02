/*
 * Copyright 2019-2024 the original author or authors.
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

import java.util.Objects;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Column name within a {@code SELECT … FROM} clause.
 * <p>
 * Renders to: {@code <name>} or {@code <table(alias)>.<name>}.
 * </p>
 * 
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
public class Column extends AbstractSegment implements Expression, Named {

	private final SqlIdentifier name;
	private final TableLike table;

	Column(String name, TableLike table) {

		super(table);
		Assert.notNull(name, "Name must not be null");

		this.name = SqlIdentifier.unquoted(name);
		this.table = table;
	}

	Column(SqlIdentifier name, TableLike table) {

		super(table);
		Assert.notNull(name, "Name must not be null");

		this.name = name;
		this.table = table;
	}

	/**
	 * Creates a new {@link Column} associated with a {@link Table}.
	 *
	 * @param name column name, must not {@literal null} or empty.
	 * @param table the table, must not be {@literal null}.
	 * @return the new {@link Column}.
	 * @since 2.3
	 */
	public static Column create(String name, TableLike table) {

		Assert.hasText(name, "Name must not be null or empty");
		Assert.notNull(table, "Table must not be null");

		return new Column(SqlIdentifier.unquoted(name), table);
	}

	/**
	 * Creates a new {@link Column} associated with a {@link Table}.
	 *
	 * @param name column name, must not {@literal null}.
	 * @param table the table, must not be {@literal null}.
	 * @return the new {@link Column}.
	 * @since 2.0
	 */
	public static Column create(SqlIdentifier name, Table table) {

		Assert.notNull(name, "Name must not be null");
		Assert.notNull(table, "Table must not be null");

		return new Column(name, table);
	}

	/**
	 * Creates a new aliased {@link Column} associated with a {@link Table}.
	 *
	 * @param name column name, must not {@literal null} or empty.
	 * @param table the table, must not be {@literal null}.
	 * @param alias column alias name, must not {@literal null} or empty.
	 * @return the new {@link Column}.
	 */
	public static Column aliased(String name, Table table, String alias) {

		Assert.hasText(name, "Name must not be null or empty");
		Assert.notNull(table, "Table must not be null");
		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedColumn(name, table, alias);
	}

	/**
	 * Creates a new aliased {@link Column}.
	 *
	 * @param alias column alias name, must not {@literal null} or empty.
	 * @return the aliased {@link Column}.
	 */
	public Column as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedColumn(name, table, SqlIdentifier.unquoted(alias));
	}

	/**
	 * Creates a new aliased {@link Column}.
	 *
	 * @param alias column alias name, must not {@literal null}.
	 * @return the aliased {@link Column}.
	 * @since 2.0
	 */
	public Column as(SqlIdentifier alias) {

		Assert.notNull(alias, "Alias must not be null");

		return new AliasedColumn(name, table, alias);
	}

	/**
	 * Creates a new {@link Column} associated with a {@link Table}.
	 *
	 * @param table the table, must not be {@literal null}.
	 * @return a new {@link Column} associated with {@link Table}.
	 */
	public Column from(Table table) {

		Assert.notNull(table, "Table must not be null");

		return new Column(name, table);
	}

	// -------------------------------------------------------------------------
	// Methods for Condition creation.
	// -------------------------------------------------------------------------

	/**
	 * Creates a {@code =} (equals) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isEqualTo(Expression expression) {
		return Conditions.isEqual(this, expression);
	}

	/**
	 * Creates a {@code !=} (not equals) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isNotEqualTo(Expression expression) {
		return Conditions.isNotEqual(this, expression);
	}

	/**
	 * Creates a {@code BETWEEN} {@link Condition}.
	 *
	 * @param begin begin value for the comparison.
	 * @param end end value for the comparison.
	 * @return the {@link Between} condition.
	 * @since 2.0
	 */
	public Between between(Expression begin, Expression end) {
		return Conditions.between(this, begin, end);
	}

	/**
	 * Creates a {@code NOT BETWEEN} {@link Condition}.
	 *
	 * @param begin begin value for the comparison.
	 * @param end end value for the comparison.
	 * @return the {@link Between} condition.
	 * @since 2.0
	 */
	public Between notBetween(Expression begin, Expression end) {
		return Conditions.notBetween(this, begin, end);
	}

	/**
	 * Creates a {@code <} (less) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isLess(Expression expression) {
		return Conditions.isLess(this, expression);
	}

	/**
	 * CCreates a {@code <=} (greater) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isLessOrEqualTo(Expression expression) {
		return Conditions.isLessOrEqualTo(this, expression);
	}

	/**
	 * Creates a {@code !=} (not equals) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isGreater(Expression expression) {
		return Conditions.isGreater(this, expression);
	}

	/**
	 * Creates a {@code <=} (greater or equal to) {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isGreaterOrEqualTo(Expression expression) {
		return Conditions.isGreaterOrEqualTo(this, expression);
	}

	/**
	 * Creates a {@code LIKE} {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Like} condition.
	 */
	public Like like(Expression expression) {
		return Conditions.like(this, expression);
	}

	/**
	 * Creates a {@code NOT LIKE} {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Like} condition.
	 * @since 2.0
	 */
	public Like notLike(Expression expression) {
		return Conditions.notLike(this, expression);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given right {@link Expression}s.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link In} condition.
	 */
	public In in(Expression... expression) {
		return Conditions.in(this, expression);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given a subselects.
	 *
	 * @param subselect right side of the comparison.
	 * @return the {@link In} condition.
	 */
	public In in(Select subselect) {
		return Conditions.in(this, subselect);
	}

	/**
	 * Creates a new {@code not} {@link In} {@link Condition} given right {@link Expression}s.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link In} condition.
	 */
	public In notIn(Expression... expression) {
		return Conditions.notIn(this, expression);
	}

	/**
	 * Creates a new {@code not} {@link In} {@link Condition} given a subselects.
	 *
	 * @param subselect right side of the comparison.
	 * @return the {@link In} condition.
	 */
	public In notIn(Select subselect) {
		return Conditions.notIn(this, subselect);
	}

	/**
	 * Creates a {@code IS NULL} condition.
	 *
	 * @return the {@link IsNull} condition.
	 */
	public IsNull isNull() {
		return Conditions.isNull(this);
	}

	/**
	 * Creates a {@code IS NOT NULL} condition.
	 *
	 * @return the {@link Condition} condition.
	 */
	public Condition isNotNull() {
		return isNull().not();
	}

	// -------------------------------------------------------------------------
	// Methods for Assignment creation.
	// -------------------------------------------------------------------------

	/**
	 * Creates a value {@link AssignValue assignment}.
	 *
	 * @param value the value to assign.
	 * @return the {@link AssignValue} assignment.
	 */
	public AssignValue set(Expression value) {
		return Assignments.value(this, value);
	}

	@Override
	public SqlIdentifier getName() {
		return name;
	}

	/**
	 * @return the column name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	public SqlIdentifier getReferenceName() {
		return getName();
	}

	/**
	 * @return the {@link Table}. Can be {@literal null} if the column was not referenced in the context of a
	 *         {@link Table}.
	 */
	@Nullable
	public TableLike getTable() {
		return table;
	}

	@Override
	public String toString() {

		return getPrefix() + name;
	}

	String getPrefix() {
		String prefix = "";
		if (table != null) {
			prefix = (table instanceof Aliased ? ((Aliased) table).getAlias() : table.getName()) + ".";
		}
		return prefix;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		Column column = (Column) o;
		return name.equals(column.name) && table.equals(column.table);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), name, table);
	}

	/**
	 * {@link Aliased} {@link Column} implementation.
	 */
	static class AliasedColumn extends Column implements Aliased {

		private final SqlIdentifier alias;

		private AliasedColumn(String name, TableLike table, String alias) {
			super(name, table);
			this.alias = SqlIdentifier.unquoted(alias);
		}

		private AliasedColumn(SqlIdentifier name, TableLike table, SqlIdentifier alias) {
			super(name, table);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}

		@Override
		public SqlIdentifier getReferenceName() {
			return getAlias();
		}

		@Override
		public Column from(Table table) {

			Assert.notNull(table, "Table must not be null");

			return new AliasedColumn(getName(), table, getAlias());
		}

		@Override
		public String toString() {
			return getPrefix() + getName() + " AS " + getAlias();
		}

		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			AliasedColumn that = (AliasedColumn) o;
			return alias.equals(that.alias);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), alias);
		}
	}
}
