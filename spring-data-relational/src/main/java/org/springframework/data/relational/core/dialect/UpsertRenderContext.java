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
package org.springframework.data.relational.core.dialect;

import java.util.List;
import java.util.function.Function;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

/**
 * Encapsulates dialect-specific rendering of a single-statement upsert (insert or update by id).
 * Implementations produce vendor-specific SQL such as {@code INSERT ... ON CONFLICT ... DO UPDATE},
 * {@code INSERT ... ON DUPLICATE KEY UPDATE}, or standard {@code MERGE}.
 *
 * @since 4.x
 */
public interface UpsertRenderContext {

	/**
	 * Whether this dialect supports a single-statement upsert.
	 *
	 * @return {@literal true} if upsert is supported.
	 */
	boolean supportsUpsert();

	/**
	 * Render a full upsert statement.
	 *
	 * @param table the target table.
	 * @param insertColumns column names for INSERT (order preserved for VALUES clause).
	 * @param conflictColumns columns that define the conflict (e.g. primary key).
	 * @param bindMarkerFn function from column name to bind marker placeholder (e.g. {@code "id" -> ":id"}).
	 * @param identifierProcessing identifier processing for rendering table and column names to SQL.
	 * @return the full upsert SQL statement.
	 */
	String renderUpsert(Table table, List<SqlIdentifier> insertColumns, List<SqlIdentifier> conflictColumns,
			Function<SqlIdentifier, String> bindMarkerFn, IdentifierProcessing identifierProcessing);
}
