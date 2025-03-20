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
package org.springframework.data.jdbc.repository.query;

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

/**
 * Utility to get from path to SQL DSL elements. This is a temporary class and duplicates parts of
 * {@link org.springframework.data.jdbc.core.convert.SqlContext}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @since 2.0
 */
class SqlContext {

	Table getTable(AggregatePath path) {

		Table table = getUnaliasedTable(path);
		AggregatePath.TableInfo tableInfo = path.getTableInfo();
		SqlIdentifier tableAlias = tableInfo.tableAlias();
		return tableAlias == null ? table : table.as(tableAlias);
	}

	Column getColumn(AggregatePath path) {

		AggregatePath.ColumnInfo columnInfo = path.getColumnInfo();
		return getTable(path).column(columnInfo.name()).as(columnInfo.alias());
	}

	Column getAnyReverseColumn(AggregatePath path) {

		AggregatePath.ColumnInfo anyReverseColumnInfo = path.getTableInfo().backReferenceColumnInfos().any();
		return getTable(path).column(anyReverseColumnInfo.name()).as(anyReverseColumnInfo.alias());
	}

	public Table getUnaliasedTable(AggregatePath path) {

		AggregatePath.TableInfo tableInfo = path.getTableInfo();
		return Table.create(tableInfo.qualifiedTableName());
	}
}
