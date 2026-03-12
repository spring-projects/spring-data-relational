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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.UpsertBuilder.BuildUpsert;

/**
 * @author Christoph Strobl
 */
public class UpsertBuilderUnitTests {

	@Test // GH-493
	void buildErrorsWhenConflictColumnsNotPartOfInsert() {

		Table table = SQL.table("users");
		Column idColumn = table.column("id");
		Column usernameColumn = table.column("name");

		BuildUpsert builder = StatementBuilder.upsert(table).insert(usernameColumn.set(SQL.bindMarker()))
				.onConflict(idColumn).update();

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(builder::build);
	}

	@Test // GH-493
	public void toStringShouldRenderAnsiMergeStatement() {

		Table table = SQL.table("users");
		Column idColumn = table.column("id");
		Column usernameColumn = table.column("name");

		Upsert upsert = StatementBuilder.upsert(table)
				.insert(idColumn.set(SQL.bindMarker()), usernameColumn.set(SQL.bindMarker())).onConflict(idColumn).update()
				.build();

		String mergeStatement = upsert.toString();

		assertThat(mergeStatement).startsWith("MERGE INTO users \"_t\"").containsSubsequence( //
				"USING (VALUES (?, ?)) AS \"_s\" (id, name)", //
				"ON _t.id = _s.id", //
				"WHEN MATCHED THEN UPDATE SET", //
				"_t.name = _s.name", //
				"WHEN NOT MATCHED THEN INSERT (id, name) VALUES (_s.id, _s.name)");
	}

	@Test // GH-493
	public void fromInsertToUpsert() {

		Table table = SQL.table("users");
		Column idColumn = table.column("id");
		Column usernameColumn = table.column("name");

		Upsert upsert = StatementBuilder.insert().into(table) //
				.columns(idColumn, usernameColumn) //
				.values(SQL.bindMarker(), SQL.literalOf("chris")) //
				.onConflict(idColumn) //
				.update() //
				.build();

		String mergeStatement = upsert.toString();

		assertThat(mergeStatement).startsWith("MERGE INTO users \"_t\"").containsSubsequence( //
				"USING (VALUES (?, 'chris')) AS \"_s\" (id, name)", //
				"ON _t.id = _s.id", //
				"WHEN MATCHED THEN UPDATE SET", //
				"_t.name = _s.name", //
				"WHEN NOT MATCHED THEN INSERT (id, name) VALUES (_s.id, _s.name)");
	}

}
