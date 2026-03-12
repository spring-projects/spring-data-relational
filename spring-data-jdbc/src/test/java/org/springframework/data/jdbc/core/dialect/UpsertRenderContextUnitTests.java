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
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.UpsertRenderContext;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UpsertRenderContext} implementations.
 */
class UpsertRenderContextUnitTests {

	private static final Table TABLE = Table.create(SqlIdentifier.unquoted("my_table"));
	private static final List<SqlIdentifier> INSERT_COLUMNS = List.of(SqlIdentifier.unquoted("id"),
			SqlIdentifier.unquoted("name"));
	private static final List<SqlIdentifier> CONFLICT_COLUMNS = List.of(SqlIdentifier.unquoted("id"));
	private static final Function<SqlIdentifier, String> BIND_MARKER = id -> ":" + id.getReference();
	private static final IdentifierProcessing IDENTIFIER_PROCESSING = IdentifierProcessing.ANSI;

	@Test // GH-493
	void postgresUpsertRendersInsertOnConflictDoUpdate() {

		String sql = PostgresUpsertRenderContext.INSTANCE.renderUpsert(TABLE, INSERT_COLUMNS, CONFLICT_COLUMNS,
				BIND_MARKER, IDENTIFIER_PROCESSING);

		assertThat(sql).startsWith("INSERT INTO");
		assertThat(sql).contains("my_table");
		assertThat(sql).contains("id");
		assertThat(sql).contains("name");
		assertThat(sql).contains(":id");
		assertThat(sql).contains(":name");
		assertThat(sql).contains("ON CONFLICT (");
		assertThat(sql).contains("DO UPDATE SET");
		assertThat(sql).contains("EXCLUDED");
	}

	@Test // GH-493
	void mergeUpsertRendersMergeInto() {

		String sql = MergeUpsertRenderContext.INSTANCE.renderUpsert(TABLE, INSERT_COLUMNS, CONFLICT_COLUMNS,
				BIND_MARKER, IDENTIFIER_PROCESSING);

		assertThat(sql).isEqualTo(
				"MERGE INTO my_table t USING (VALUES (:id, :name)) AS s (id, name) ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)");
	}

	@Test // GH-493
	void mySqlUpsertRendersOnDuplicateKeyUpdate() {

		String sql = MySqlUpsertRenderContext.INSTANCE.renderUpsert(TABLE, INSERT_COLUMNS, CONFLICT_COLUMNS,
				BIND_MARKER, IDENTIFIER_PROCESSING);

		assertThat(sql).startsWith("INSERT INTO");
		assertThat(sql).contains("my_table");
		assertThat(sql).contains("ON DUPLICATE KEY UPDATE");
		assertThat(sql).contains("VALUES(");
	}

	@Test // GH-493
	void oracleMergeUpsertRendersOnConditionInParentheses() {

		String sql = OracleMergeUpsertRenderContext.INSTANCE.renderUpsert(TABLE, INSERT_COLUMNS, CONFLICT_COLUMNS,
				BIND_MARKER, IDENTIFIER_PROCESSING);

		assertThat(sql).startsWith("MERGE INTO");
		assertThat(sql).contains("USING (SELECT");
		assertThat(sql).contains("FROM DUAL");
		// Oracle requires ON condition in parentheses (ORA-00969 otherwise).
		assertThat(sql).contains("ON (t.id = s.id)");
		assertThat(sql).contains("WHEN MATCHED THEN UPDATE SET");
		assertThat(sql).contains("WHEN NOT MATCHED THEN INSERT");
	}
}
