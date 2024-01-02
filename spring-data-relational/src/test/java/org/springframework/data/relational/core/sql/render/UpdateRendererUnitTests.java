/*
 * Copyright 2019-2024 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Update;

/**
 * Unit tests for {@link SqlRenderer}.
 *
 * @author Mark Paluch
 */
public class UpdateRendererUnitTests {

	@Test // DATAJDBC-335
	public void shouldRenderSimpleUpdate() {

		Table table = SQL.table("mytable");
		Column column = table.column("foo");

		Update update = StatementBuilder.update(table).set(column.set(SQL.bindMarker())).build();

		assertThat(SqlRenderer.toString(update)).isEqualTo("UPDATE mytable SET foo = ?");
	}

	@Test // DATAJDBC-335
	public void shouldRenderMultipleColumnUpdate() {

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Update update = StatementBuilder.update(table) //
				.set(foo.set(SQL.bindMarker()), bar.set(SQL.bindMarker())) //
				.build();

		assertThat(SqlRenderer.toString(update)).isEqualTo("UPDATE mytable SET foo = ?, bar = ?");
	}

	@Test // DATAJDBC-335
	public void shouldRenderUpdateWithLiteral() {

		Table table = SQL.table("mytable");
		Column column = table.column("foo");

		Update update = StatementBuilder.update(table).set(column.set(SQL.literalOf(20))).build();

		assertThat(SqlRenderer.toString(update)).isEqualTo("UPDATE mytable SET foo = 20");
	}

	@Test // DATAJDBC-335
	public void shouldCreateUpdateWIthCondition() {

		Table table = SQL.table("mytable");
		Column column = table.column("foo");

		Update update = StatementBuilder.update(table).set(column.set(SQL.bindMarker())).where(column.isNull()).build();

		assertThat(SqlRenderer.toString(update)).isEqualTo("UPDATE mytable SET foo = ? WHERE mytable.foo IS NULL");
	}
}
