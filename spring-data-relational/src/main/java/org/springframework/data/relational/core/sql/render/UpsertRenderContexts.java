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
package org.springframework.data.relational.core.sql.render;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.1
 */
public enum UpsertRenderContexts implements UpsertRenderContext {

	/**
	 * MySQL / MariaDB upsert using {@code INSERT ... ON DUPLICATE KEY UPDATE}.
	 */
	MYSQL,

	/**
	 * Oracle MERGE upsert. Uses {@code SELECT ... FROM DUAL} for source values.
	 */
	ORACLE,

	/**
	 * SQL Server MERGE upsert. The statement body matches {@link UpsertStatementRenderers.Merge} with a trailing
	 * semicolon.
	 */
	SQL_SERVER,

	/**
	 * Standard SQL {@code MERGE} upsert for dialects that support it (like H2, HSQLDB, SQL Server, DB2).
	 * <p>
	 * Uses a table value constructor {@code (VALUES (?, ?)) AS s (col1, col2)} as the source so that no SELECT is used.c
	 */
	MERGE,

	/**
	 * Unsupported dialect.
	 */
	UNSUPPORTED {
		@Override
		public boolean supportsUpsert() {
			return false;
		}
	};

	@Override
	public boolean supportsUpsert() {
		return true;
	}

}
