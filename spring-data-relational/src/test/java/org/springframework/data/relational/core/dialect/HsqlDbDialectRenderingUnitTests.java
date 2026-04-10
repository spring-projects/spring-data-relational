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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Upsert;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Tests for {@link HsqlDbDialect}-specific rendering.
 *
 * @author Christoph Strobl
 */
class HsqlDbDialectRenderingUnitTests {

	private final RenderContextFactory factory = new RenderContextFactory(new HsqlDbDialect());

	@Test // GH-493
	void rendersUpsertHappyPath() {

		Table table = Table.create("my_table");
		Column idColumn = table.column("id");
		Column nameColumn = table.column("name");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(idColumn.set(SQL.bindMarker(":id")), nameColumn.set(SQL.bindMarker(":name")))
				.onConflict(it -> it.with(idColumn).updateRemainingColumns()).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(upsert);

		assertThat(sql).isEqualTo(
				"MERGE INTO my_table \"_t\" USING (VALUES (:id, :name)) AS \"_s\" (id, name) ON \"_t\".id = \"_s\".id WHEN MATCHED THEN UPDATE SET \"_t\".name = \"_s\".name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (\"_s\".id, \"_s\".name)");
	}

	@Test // GH-493
	void rendersUpsertWithValues() {

		Table table = Table.create("my_table");
		Column idColumn = table.column("id");
		Column nameColumn = table.column("name");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(idColumn.set(SQL.literalOf(42)), nameColumn.set(SQL.literalOf("batman")))
				.onConflict(it -> it.with(idColumn).updateRemainingColumns())
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(upsert);

		assertThat(sql).isEqualTo(
				"MERGE INTO my_table \"_t\" USING (VALUES (42, 'batman')) AS \"_s\" (id, name) ON \"_t\".id = \"_s\".id WHEN MATCHED THEN UPDATE SET \"_t\".name = \"_s\".name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (\"_s\".id, \"_s\".name)");
	}

	@Test // GH-493
	void rendersUpsertWhereConflictColumnsMatchInsertColumns() { // omits `WHEN MATCHED`

		Table table = Table.create("my_table");
		Column idColumn = table.column("id");
		Column tenantColumn = table.column("tenant_id");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(idColumn.set(SQL.bindMarker(":id")), tenantColumn.set(SQL.bindMarker(":tenant_id")))
				.onConflict(it -> it.with(idColumn, tenantColumn).updateRemainingColumns()).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(upsert);

		assertThat(sql).isEqualTo(
				"MERGE INTO my_table \"_t\" USING (VALUES (:id, :tenant_id)) AS \"_s\" (id, tenant_id) ON \"_t\".id = \"_s\".id AND \"_t\".tenant_id = \"_s\".tenant_id WHEN NOT MATCHED THEN INSERT (id, tenant_id) VALUES (\"_s\".id, \"_s\".tenant_id)");
	}

}
