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
 * PostgreSQL upsert using {@code INSERT ... ON CONFLICT ... DO UPDATE SET}.
 *
 * @since 4.x
 */
public enum PostgresUpsertRenderContext implements UpsertRenderContext {

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

		String columnsSql = String.join(", ",
				insertColumns.stream().map(col -> col.toSql(identifierProcessing)).toList());

		String valuesSql = String.join(", ", insertColumns.stream().map(bindMarkerFn).toList());

		String conflictSql = String.join(", ",
				conflictColumns.stream().map(col -> col.toSql(identifierProcessing)).toList());

		List<SqlIdentifier> updateColumns = insertColumns.stream()
				.filter(col -> !conflictSet.contains(col))
				.toList();

		String setClause;
		if (updateColumns.isEmpty()) {
			// PostgreSQL requires at least one SET; use conflict column as no-op
			SqlIdentifier firstConflict = conflictColumns.get(0);
			setClause = firstConflict.toSql(identifierProcessing) + " = EXCLUDED."
					+ firstConflict.toSql(identifierProcessing);
		} else {
			setClause = String.join(", ", updateColumns.stream()
					.map(col -> col.toSql(identifierProcessing) + " = EXCLUDED." + col.toSql(identifierProcessing))
					.toList());
		}

		return "INSERT INTO " + tableSql + " (" + columnsSql + ") VALUES (" + valuesSql + ")"
				+ " ON CONFLICT (" + conflictSql + ") DO UPDATE SET " + setClause;
	}
}
