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
package org.springframework.data.relational.core.dialect;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Tests for {@link SqlServerDialect}-specific rendering.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
public class SqlServerDialectRenderingUnitTests {

	private final RenderContextFactory factory = new RenderContextFactory(SqlServerDialect.INSTANCE);

	@BeforeEach
	public void before() {
		factory.setNamingStrategy(NamingStrategies.asIs());
	}

	@Test // DATAJDBC-278
	public void shouldRenderSimpleSelect() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo");
	}

	@Test // DATAJDBC-278
	public void shouldApplyNamingStrategy() {

		factory.setNamingStrategy(NamingStrategies.toUpper());

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT FOO.* FROM FOO");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithLimit() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
				"SELECT foo.*, ROW_NUMBER() over (ORDER BY (SELECT 1)) AS __relational_row_number__ FROM foo ORDER BY __relational_row_number__ OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithOffset() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).offset(10).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
				"SELECT foo.*, ROW_NUMBER() over (ORDER BY (SELECT 1)) AS __relational_row_number__ FROM foo ORDER BY __relational_row_number__ OFFSET 10 ROWS");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithLimitOffset() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).offset(20).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
				"SELECT foo.*, ROW_NUMBER() over (ORDER BY (SELECT 1)) AS __relational_row_number__ FROM foo ORDER BY __relational_row_number__ OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithLimitOffsetAndOrderBy() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).orderBy(table.column("column_1")).limit(10)
				.offset(20).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.column_1 OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
			"SELECT foo.* FROM foo WITH (UPDLOCK, ROWLOCK)");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
			"SELECT foo.* FROM foo WITH (HOLDLOCK, ROWLOCK)");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitOffsetWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).offset(20).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
			"SELECT foo.*, ROW_NUMBER() over (ORDER BY (SELECT 1)) AS __relational_row_number__ FROM foo WITH (UPDLOCK, ROWLOCK) ORDER BY __relational_row_number__ OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitOffsetWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).offset(20).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo(
			"SELECT foo.*, ROW_NUMBER() over (ORDER BY (SELECT 1)) AS __relational_row_number__ FROM foo WITH (HOLDLOCK, ROWLOCK) ORDER BY __relational_row_number__ OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitOffsetAndOrderByWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).orderBy(table.column("column_1")).limit(10)
			.offset(20).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo WITH (UPDLOCK, ROWLOCK) ORDER BY foo.column_1 OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitOffsetAndOrderByWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).orderBy(table.column("column_1")).limit(10)
			.offset(20).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo WITH (HOLDLOCK, ROWLOCK) ORDER BY foo.column_1 OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
	}

	@Test // GH-821
	void shouldRenderSelectOrderByIgnoringNullHandling() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk())
				.from(table)
				.orderBy(OrderByField.from(Column.create("bar", table))
						.withNullHandling(Sort.NullHandling.NULLS_FIRST))
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.bar");
	}
}
