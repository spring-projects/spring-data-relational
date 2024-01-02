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
package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.util.Assert;

/**
 * A segment that can be used as table in a query.
 *
 * @author Jens Schauder
 * @Since 2.3
 */
public interface TableLike extends Segment {
	/**
	 * Creates a new {@link Column} associated with this {@link Table}.
	 * <p>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param name column name, must not be {@literal null} or empty.
	 * @return a new {@link Column} associated with this {@link Table}.
	 */
	default Column column(String name) {

		Assert.hasText(name, "Name must not be null or empty");

		return new Column(name, this);
	}

	/**
	 * Creates a new {@link Column} associated with this {@link Table}.
	 * <p>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param name column name, must not be {@literal null} or empty.
	 * @return a new {@link Column} associated with this {@link Table}.
	 * @since 2.0
	 */
	default Column column(SqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");

		return new Column(name, this);
	}

	/**
	 * Creates a {@link List} of {@link Column}s associated with this {@link Table}.
	 * <p>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param names column names, must not be {@literal null} or empty.
	 * @return a new {@link List} of {@link Column}s associated with this {@link Table}.
	 */
	default List<Column> columns(String... names) {

		Assert.notNull(names, "Names must not be null");

		return columns(Arrays.asList(names));
	}

	/**
	 * Creates a {@link List} of {@link Column}s associated with this {@link Table}.
	 * <p>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param names column names, must not be {@literal null} or empty.
	 * @return a new {@link List} of {@link Column}s associated with this {@link Table}.
	 * @since 2.0
	 */
	default List<Column> columns(SqlIdentifier... names) {

		Assert.notNull(names, "Names must not be null");

		List<Column> columns = new ArrayList<>();
		for (SqlIdentifier name : names) {
			columns.add(column(name));
		}

		return columns;
	}

	/**
	 * Creates a {@link List} of {@link Column}s associated with this {@link Table}.
	 * <p>
	 * Note: This {@link Table} does not track column creation and there is no possibility to enumerate all
	 * {@link Column}s that were created for this table.
	 *
	 * @param names column names, must not be {@literal null} or empty.
	 * @return a new {@link List} of {@link Column}s associated with this {@link Table}.
	 */
	default List<Column> columns(Collection<String> names) {

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
	default AsteriskFromTable asterisk() {
		return new AsteriskFromTable(this);
	}

	/**
	 * @return the table name.
	 */
	SqlIdentifier getName();

	/**
	 * @return the table name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	SqlIdentifier getReferenceName();
}
