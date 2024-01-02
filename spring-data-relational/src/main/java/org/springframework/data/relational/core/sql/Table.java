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
 * Represents a table reference within a SQL statement. Typically, used to denote {@code FROM} or {@code JOIN} or to
 * prefix a {@link Column}.
 * <p>
 * Renders to: {@code <name>} or {@code <name> AS <name>}.
 * </p>
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
public class Table extends AbstractSegment implements TableLike {

	private final SqlIdentifier name;

	Table(String name) {
		this.name = SqlIdentifier.unquoted(name);
	}

	Table(SqlIdentifier name) {
		this.name = name;
	}

	/**
	 * Creates a new {@link Table} given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return the new {@link Table}.
	 */
	public static Table create(String name) {

		Assert.hasText(name, "Name must not be null or empty");

		return new Table(name);
	}

	/**
	 * Creates a new {@link Table} given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return the new {@link Table}.
	 * @since 2.0
	 */
	public static Table create(SqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");

		return new Table(name);
	}

	/**
	 * Creates a new {@link Table} using an {@code alias}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link Table} using the {@code alias}.
	 */
	public static Table aliased(String name, String alias) {

		Assert.hasText(name, "Name must not be null or empty");
		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedTable(name, alias);
	}

	/**
	 * Creates a new {@link Table} aliased to {@code alias}.
	 *
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link Table} using the {@code alias}.
	 */
	public Table as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedTable(name, SqlIdentifier.unquoted(alias));
	}

	/**
	 * Creates a new {@link Table} aliased to {@code alias}.
	 *
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link Table} using the {@code alias}.
	 * @since 2.0
	 */
	public Table as(SqlIdentifier alias) {

		Assert.notNull(alias, "Alias must not be null");

		return new AliasedTable(name, alias);
	}

	@Override
	public SqlIdentifier getName() {
		return name;
	}

	/**
	 * @return the table name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	@Override
	public SqlIdentifier getReferenceName() {
		return name;
	}

	@Override
	public String toString() {
		return name.toString();
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
		Table table = (Table) o;
		return name.equals(table.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), name);
	}

	/**
	 * {@link Aliased} {@link Table} implementation.
	 */
	static class AliasedTable extends Table implements Aliased {

		private final SqlIdentifier alias;

		AliasedTable(String name, String alias) {
			super(name);

			Assert.hasText(alias, "Alias must not be null or empty");

			this.alias = SqlIdentifier.unquoted(alias);
		}

		AliasedTable(SqlIdentifier name, SqlIdentifier alias) {
			super(name);

			Assert.notNull(alias, "Alias must not be null");

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
		public String toString() {
			return getName() + " AS " + getAlias();
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
			AliasedTable that = (AliasedTable) o;
			return alias.equals(that.alias);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), alias);
		}
	}
}
