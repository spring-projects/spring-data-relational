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

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.UpsertStatementRenderer.UpsertRenderingContext;

/**
 * Unit tests for {@link StandardSqlUpsertRenderContext}.
 */
class MergeUpsertRenderContextUnitTests {

	private static final Table TABLE = Table.create("my_table");

	@Test // GH-493
	void mergeUpsertWithMultipleConflictColumnsBuildsFilterClauseWithAllColumns() {

		List<String> insertColumns = List.of("tenant_id", "id", "name");
		List<String> conflictColumns = List.of("tenant_id", "id");
		List<String> updateColumns = List.of("name");

		UpsertRenderingContext ctx = UpsertRenderingContext.of(
				new RenderContextFactory(AnsiDialect.INSTANCE).createRenderContext(), it -> ":%s".formatted(it.getReference()));

		List<Column> insertCols = getColumns(insertColumns);
		List<Column> conflictCols = getColumns(conflictColumns);
		List<Column> updateCols = getColumns(updateColumns);
		Map<SqlIdentifier, CharSequence> bindings = Map.of(insertCols.get(0).getName(),
				insertCols.get(0).getName().getReference());

		String sql = UpsertStatementRenderers.merge().render(TABLE,
				new UpsertStatementRenderer.Columns(insertCols, conflictCols, updateCols, bindings), ctx);

		assertThat(sql).contains("ON \"_t\".tenant_id = \"_s\".tenant_id AND \"_t\".id = \"_s\".id");
		assertThat(sql).contains("WHEN MATCHED THEN UPDATE SET \"_t\".name = \"_s\".name");
		assertThat(sql).contains(
				"WHEN NOT MATCHED THEN INSERT (tenant_id, id, name) VALUES (\"_s\".tenant_id, \"_s\".id, \"_s\".name)");
	}

	private List<Column> getColumns(List<String> insertColumns) {
		return insertColumns.stream().map(id -> Column.create(id, TABLE)).toList();
	}

}
