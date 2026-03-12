/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.springframework.data.relational.core.dialect.UpsertRenderContext;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.util.Assert;

/**
 * Standard SQL {@code MERGE} upsert for dialects that support it (H2, HSQLDB, SQL Server, DB2).
 * <p>
 * Uses a table value constructor {@code (VALUES (?, ?)) AS s (col1, col2)} as the source so that
 * no SELECT is used. This avoids H2 interpreting {@code ? AS "ID"} as a type cast (unknown data
 * type "ID"), and avoids HSQLDB requiring a FROM clause on SELECT.
 *
 * @since 4.x
 */
public enum MergeUpsertRenderContext implements UpsertRenderContext {

	INSTANCE;

	@Override
	public boolean supportsUpsert() {
		return true;
	}

	@Override
	public String renderUpsert(Table table, List<SqlIdentifier> insertColumns, List<SqlIdentifier> conflictColumns,
			Function<SqlIdentifier, String> bindMarkerFn, IdentifierProcessing identifierProcessing) {

		Assert.notEmpty(insertColumns, "Insert columns must not be empty");
		Assert.notEmpty(conflictColumns, "Conflict columns must not be empty");

		Set<SqlIdentifier> conflictSet = Set.copyOf(conflictColumns);
		String tableSql = table.getName().toSql(identifierProcessing);

		String valuesList = String.join(", ", insertColumns.stream().map(bindMarkerFn).toList());
		String sourceColumnsSql = String.join(", ",
				insertColumns.stream().map(col -> col.toSql(identifierProcessing)).toList());

		String onCondition = String.join(" AND ", conflictColumns.stream()
				.map(col -> "t." + col.toSql(identifierProcessing) + " = s." + col.toSql(identifierProcessing))
				.toList());

		List<SqlIdentifier> updateColumns = insertColumns.stream()
				.filter(col -> !conflictSet.contains(col))
				.toList();

		String updateSetClause;
		if (updateColumns.isEmpty()) {
			SqlIdentifier firstConflict = conflictColumns.get(0);
			updateSetClause = "t." + firstConflict.toSql(identifierProcessing) + " = s."
					+ firstConflict.toSql(identifierProcessing);
		} else {
			updateSetClause = String.join(", ", updateColumns.stream()
					.map(col -> "t." + col.toSql(identifierProcessing) + " = s." + col.toSql(identifierProcessing))
					.toList());
		}

		String insertColumnsSql = String.join(", ",
				insertColumns.stream().map(col -> col.toSql(identifierProcessing)).toList());

		String insertValuesSql = String.join(", ",
				insertColumns.stream().map(col -> "s." + col.toSql(identifierProcessing)).toList());

		return "MERGE INTO " + tableSql + " t USING (VALUES (" + valuesList + ")) AS s (" + sourceColumnsSql + ") ON "
				+ onCondition
				+ " WHEN MATCHED THEN UPDATE SET " + updateSetClause
				+ " WHEN NOT MATCHED THEN INSERT (" + insertColumnsSql + ") VALUES (" + insertValuesSql + ")";
	}
}
