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
package org.springframework.data.jdbc.core.sql.render;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.dialect.JdbcOracleDialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Upsert;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Unit tests for rendering {@link Upsert} AST via {@link SqlRenderer} with dialect-specific
 * {@link org.springframework.data.relational.core.sql.render.UpsertRenderContext}.
 */
class UpsertRendererUnitTests {

	@Test // GH-493
	void standardSqlUpsertUsesMerge() {

		Table table = SQL.table("my_table");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(table.column("id").set(SQL.bindMarker(":id")), table.column("name").set(SQL.bindMarker(":name")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(org.springframework.data.jdbc.core.convert.NonQuotingDialect.INSTANCE)
				.createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"MERGE INTO my_table \"_t\" USING (VALUES (:id, :name)) AS \"_s\" (id, name) ON _t.id = _s.id WHEN MATCHED THEN UPDATE SET _t.name = _s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (_s.id, _s.name)");
	}

	@Test // GH-493
	void postgresRendersInsertOnConflictDoUpdate() {

		Table table = SQL.table("my_table");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(table.column("id").set(SQL.bindMarker(":id")), table.column("name").set(SQL.bindMarker(":name")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect.INSTANCE)
				.createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"INSERT INTO my_table (id, name) VALUES (:id, :name) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name");
	}

	@Test // GH-493
	void mySqlRendersOnDuplicateKeyUpdate() {

		Table table = SQL.table("my_table");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(table.column("id").set(SQL.bindMarker(":id")), table.column("name").set(SQL.bindMarker(":name")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(org.springframework.data.jdbc.core.dialect.JdbcMySqlDialect.INSTANCE)
				.createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"INSERT INTO my_table (id, name) VALUES (:id, :name) ON DUPLICATE KEY UPDATE name = VALUES(name)");
	}

	@Test // GH-493
	void sqlServerRendersMergeWithSemicolon() {

		Table table = SQL.table("my_table");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(table.column("id").set(SQL.bindMarker(":id")), table.column("name").set(SQL.bindMarker(":name")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(org.springframework.data.jdbc.core.dialect.JdbcSqlServerDialect.INSTANCE)
				.createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"MERGE INTO my_table \"_t\" USING (VALUES (:id, :name)) AS \"_s\" (id, name) ON \"_t\".id = \"_s\".id WHEN MATCHED THEN UPDATE SET \"_t\".name = \"_s\".name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (\"_s\".id, \"_s\".name);");
	}

	@Test // GH-493
	void h2RendersMerge() {

		Table table = SQL.table("my_table");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(table.column("id").set(SQL.bindMarker(":id")), table.column("name").set(SQL.bindMarker(":name")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(org.springframework.data.jdbc.core.dialect.JdbcH2Dialect.INSTANCE)
				.createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"MERGE INTO my_table \"_t\" USING (VALUES (:id, :name)) AS \"_s\" (id, name) ON \"_t\".id = \"_s\".id WHEN MATCHED THEN UPDATE SET \"_t\".name = \"_s\".name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (\"_s\".id, \"_s\".name)");
	}

	@Test // GH-493
	void oracleIdOnlyMergeOmitsWhenMatchedUpdate() {

		Table table = SQL.table("ent");
		Upsert upsert = StatementBuilder.upsert(table).insert(table.column("id").set(SQL.bindMarker(":id")))
				.onConflict(table.column("id")).update().build();

		var context = new RenderContextFactory(JdbcOracleDialect.INSTANCE).createRenderContext();
		String sql = SqlRenderer.create(context).render(upsert);

		assertThat(sql).isEqualToIgnoringWhitespace(
				"MERGE INTO ent \"_t\" USING (SELECT :id AS id FROM DUAL) \"_s\" ON (\"_t\".id = \"_s\".id) WHEN NOT MATCHED THEN INSERT (id) VALUES (\"_s\".id)");
	}
}
