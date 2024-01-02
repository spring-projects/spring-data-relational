/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import net.sf.jsqlparser.schema.Column;

import java.util.Objects;

/**
 * Pattern matching just a simple column
 *
 * @author Jens Schauder
 */
class ColumnPattern extends TypedExpressionPattern<Column> {
	private final String columnName;

	/**
	 * @param columnName name of the expected column.
	 */
	ColumnPattern(String columnName) {

		super(Column.class);

		this.columnName = columnName;
	}

	@Override
	public boolean matches(Column actualColumn) {
		return actualColumn.getColumnName().equals(columnName);
	}

	@Override
	public String toString() {
		return columnName;
	}

	public String columnName() {
		return columnName;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (ColumnPattern) obj;
		return Objects.equals(this.columnName, that.columnName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(columnName);
	}

}
