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
package org.springframework.data.relational.core.sql;

import static org.assertj.core.api.AssertionsForClassTypes.*;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.UpsertBuilder.BuildUpsert;

/**
 * Unit tests for {@link UpsertBuilder}.
 *
 * @author Christoph Strobl
 * @author YeongJae Min
 */
class UpsertBuilderUnitTests {

	@Test // GH-2277
	void conditionalUpdateRequiresColumnsToUpdate() {

		Table table = SQL.table("users");
		Column id = table.column("id");
		BuildUpsert builder = StatementBuilder.upsert(table).insert(id.set(SQL.bindMarker(":id")))
				.onConflict(it -> it.with(id).updateRemainingColumns()).where(id.isEqualTo(SQL.bindMarker(":expectedId")));

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(builder::build);
	}

	@Test // GH-493
	void buildErrorsWhenConflictColumnsNotPartOfInsert() {

		Table table = SQL.table("users");
		Column idColumn = table.column("id");
		Column usernameColumn = table.column("name");

		BuildUpsert builder = StatementBuilder.upsert(table).insert(usernameColumn.set(SQL.bindMarker()))
				.onConflict(it -> it.with(idColumn).updateRemainingColumns());

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(builder::build);
	}

	@Test // GH-493
	void toStringShouldRenderAnsiMergeStatement() {

		Table table = SQL.table("users");
		Column idColumn = table.column("id");
		Column usernameColumn = table.column("name");

		Upsert upsert = StatementBuilder.upsert(table)
				.insert(idColumn.set(SQL.bindMarker()), usernameColumn.set(SQL.bindMarker()))
				.onConflict(it -> it.with(idColumn).updateRemainingColumns())
				.build();

		String mergeStatement = upsert.toString();

		assertThat(mergeStatement).startsWith("MERGE INTO users \"_t\"").containsSubsequence( //
				"USING (VALUES (?, ?)) AS \"_s\" (id, name)", //
				"ON _t.id = _s.id", //
				"WHEN MATCHED THEN UPDATE SET", //
				"_t.name = _s.name", //
				"WHEN NOT MATCHED THEN INSERT (id, name) VALUES (_s.id, _s.name)");
	}

	@Test // GH-2277
	void toStringOfConditionalUpsertIncludesTheUpdateCondition() {

		Table table = SQL.table("users");
		Column id = table.column("id");
		Column version = table.column("version");
		Upsert upsert = StatementBuilder.upsert(table)
				.insert(id.set(SQL.bindMarker(":id")), version.set(SQL.bindMarker(":nextVersion")))
				.onConflict(it -> it.with(id).update(version)).where(version.isEqualTo(SQL.bindMarker(":previousVersion")))
				.build();

		assertThat(upsert.toString()).contains("users", "version", ":id", ":nextVersion", ":previousVersion");
	}

}
