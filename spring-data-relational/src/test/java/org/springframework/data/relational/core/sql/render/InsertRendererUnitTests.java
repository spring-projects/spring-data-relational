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
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link SqlRenderer}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class InsertRendererUnitTests {

	@Test // DATAJDBC-335
	public void shouldRenderInsert() {

		Table bar = SQL.table("bar");

		Insert insert = Insert.builder().into(bar).values(SQL.bindMarker()).build();

		assertThat(SqlRenderer.toString(insert)).isEqualTo("INSERT INTO bar VALUES (?)");
	}

	@Test // DATAJDBC-335
	public void shouldRenderInsertColumn() {

		Table bar = SQL.table("bar");

		Insert insert = Insert.builder().into(bar).column(bar.column("foo")).values(SQL.bindMarker()).build();

		assertThat(SqlRenderer.toString(insert)).isEqualTo("INSERT INTO bar (foo) VALUES (?)");
	}

	@Test // DATAJDBC-335
	public void shouldRenderInsertMultipleColumns() {

		Table bar = SQL.table("bar");

		Insert insert = Insert.builder().into(bar).columns(bar.columns("foo", "baz")).value(SQL.bindMarker())
				.value(SQL.literalOf("foo")).build();

		assertThat(SqlRenderer.toString(insert)).isEqualTo("INSERT INTO bar (foo, baz) VALUES (?, 'foo')");
	}

	@Test // DATAJDBC-340
	public void shouldRenderInsertWithZeroColumns() {

		Table bar = SQL.table("bar");

		Insert insert = Insert.builder().into(bar).build();

		assertThat(SqlRenderer.toString(insert)).isEqualTo("INSERT INTO bar VALUES (DEFAULT)");
	}

}
