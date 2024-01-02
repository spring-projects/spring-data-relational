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
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link SqlRenderer}.
 *
 * @author Mark Paluch
 */
public class DeleteRendererUnitTests {

	@Test // DATAJDBC-335
	public void shouldRenderWithoutWhere() {

		Table bar = SQL.table("bar");

		Delete delete = Delete.builder().from(bar).build();

		assertThat(SqlRenderer.toString(delete)).isEqualTo("DELETE FROM bar");
	}

	@Test // DATAJDBC-335
	public void shouldRenderWithCondition() {

		Table table = Table.create("bar");

		Delete delete = Delete.builder().from(table) //
				.where(table.column("foo").isEqualTo(table.column("baz"))) //
				.and(table.column("doe").isNull()).build();

		assertThat(SqlRenderer.toString(delete)).isEqualTo("DELETE FROM bar WHERE bar.foo = bar.baz AND bar.doe IS NULL");
	}

	@Test // DATAJDBC-335
	public void shouldConsiderTableAlias() {

		Table table = Table.create("bar").as("my_bar");

		Delete delete = Delete.builder().from(table) //
				.where(table.column("foo").isEqualTo(table.column("baz"))) //
				.build();

		assertThat(SqlRenderer.toString(delete)).isEqualTo("DELETE FROM bar my_bar WHERE my_bar.foo = my_bar.baz");
	}
}
