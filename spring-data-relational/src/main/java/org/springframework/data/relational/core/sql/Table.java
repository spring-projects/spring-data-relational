/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Represents a table reference within an SQL statement. Typically used to denote {@code FROM} or {@code JOIN} or to
 * prefix a {@link Column}.
 * <p/>
 * Renders to: {@code <name>} or {@code <name> AS <name>}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class Table extends AbstractSegment {

	private final String name;

	Table(String name) {
		super();
		this.name = name;
	}

	/**
	 * Creates a new {@link Table} given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return the new {@link Table}.
	 */
	public static Table create(String name) {

		Assert.hasText(name, "Name must not be null or empty!");

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

		Assert.hasText(name, "Name must not be null or empty!");
		Assert.hasText(alias, "Alias must not be null or empty!");

		return new AliasedTable(name, alias);
	}

	/**
	 * Creates a new {@link Table} aliased to {@code alias}.
	 *
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link Table} using the {@code alias}.
	 */
	public Table as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty!");

		return new AliasedTable(name, alias);
	}

	/**
	 * Creates a new {@link Column} associated with this {@link Table}.
	 * <p/>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param name column name, must not be {@literal null} or empty.
	 * @return a new {@link Column} associated with this {@link Table}.
	 */
	public Column column(String name) {

		Assert.hasText(name, "Name must not be null or empty!");

		return new Column(name, this);
	}

	/**
	 * Creates a {@link List} of {@link Column}s associated with this {@link Table}.
	 * <p/>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param names column names, must not be {@literal null} or empty.
	 * @return a new {@link List} of {@link Column}s associated with this {@link Table}.
	 */
	public List<Column> columns(String... names) {

		Assert.notNull(names, "Names must not be null");

		return columns(Arrays.asList(names));
	}

	/**
	 * Creates a {@link List} of {@link Column}s associated with this {@link Table}.
	 * <p/>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param names column names, must not be {@literal null} or empty.
	 * @return a new {@link List} of {@link Column}s associated with this {@link Table}.
	 */
	public List<Column> columns(Collection<String> names) {

		Assert.notNull(names, "Names must not be null");

		List<Column> columns = new ArrayList<>();
		for (String name : names) {
			columns.add(column(name));
		}

		return columns;
	}

	/**
	 * Creates a {@link AsteriskFromTable} maker selecting all columns from this {@link Table} (e.g. {@code SELECT
	 *
	<table>
	 * .*}.
	 *
	 * @return the select all marker for this {@link Table}.
	 */
	public AsteriskFromTable asterisk() {
		return new AsteriskFromTable(this);
	}

	/**
	 * @return the table name.
	 */
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Named#getName()
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the table name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	public String getReferenceName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return name;
	}

	/**
	 * {@link Aliased} {@link Table} implementation.
	 */
	static class AliasedTable extends Table implements Aliased {

		private final String alias;

		AliasedTable(String name, String alias) {
			super(name);

			Assert.hasText(alias, "Alias must not be null or empty!");

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
		 * @see org.springframework.data.relational.core.sql.Table#getReferenceName()
		 */
		@Override
		public String getReferenceName() {
			return getAlias();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Table#toString()
		 */
		@Override
		public String toString() {
			return getName() + " AS " + getAlias();
		}
	}
}
