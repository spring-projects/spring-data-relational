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

/**
 * {@link Segment} to select all columns from a {@link Table}.
 * <p>
 * Renders to: {@code
 *
<table>
 * .*} as in {@code SELECT
 *
<table>
 * .* FROM …}.
 * </p>
 *
 * @author Mark Paluch
 * @since 1.1
 * @see Table#asterisk()
 */
public class AsteriskFromTable extends AbstractSegment implements Expression {

	private final TableLike table;

	AsteriskFromTable(TableLike table) {
		super(table);
		this.table = table;
	}

	public static AsteriskFromTable create(Table table) {
		return new AsteriskFromTable(table);
	}

	/**
	 * @return the associated {@link Table}.
	 */
	public TableLike getTable() {
		return table;
	}

	@Override
	public String toString() {

		if (table instanceof Aliased) {
			return ((Aliased) table).getAlias() + ".*";
		}

		return table + ".*";
	}
}
