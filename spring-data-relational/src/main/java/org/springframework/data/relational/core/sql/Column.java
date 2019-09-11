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
package org.springframework.data.relational.core.sql;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Column name within a {@code SELECT … FROM} clause.
 * <p/>
 * Renders to: {@code <name>} or {@code <table(alias)>.<name>}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
public class Column extends AbstractSegment implements Expression, Named {

	private final String name;
	private final Table table;

	Column(String name, Table table) {

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
	 */
	public static Column create(String name, Table table) {

		Assert.hasText(name, "Name must not be null or empty");
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
	 * Creates a {@code <} (less) {@link Condition} {@link Condition}.
	 *
	 * @param expression right side of the comparison.
	 * @return the {@link Comparison} condition.
	 */
	public Comparison isLess(Expression expression) {
		return Conditions.isLess(this, expression);
	}

	/**
	 * CCreates a {@code <=} (greater ) {@link Condition} {@link Condition}.
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
	 * Creates a {@code <=} (greater or equal to) {@link Condition} {@link Condition}.
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Named#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @return the column name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	public String getReferenceName() {
		return name;
	}

	/**
	 * @return the {@link Table}. Can be {@literal null} if the column was not referenced in the context of a
	 *         {@link Table}.
	 */
	@Nullable
	public Table getTable() {
		return table;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
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

	/**
	 * {@link Aliased} {@link Column} implementation.
	 */
	static class AliasedColumn extends Column implements Aliased {

		private final String alias;

		private AliasedColumn(String name, Table table, String alias) {
			super(name, table);
			this.alias = alias;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Aliased#getAlias()
		 */
		@Override
		public String getAlias() {
			return alias;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Column#getReferenceName()
		 */
		@Override
		public String getReferenceName() {
			return getAlias();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Column#from(org.springframework.data.relational.core.sql.Table)
		 */
		@Override
		public Column from(Table table) {

			Assert.notNull(table, "Table must not be null");

			return new AliasedColumn(getName(), table, getAlias());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Column#toString()
		 */
		@Override
		public String toString() {
			return getPrefix() + getName() + " AS " + getAlias();
		}
	}
}
