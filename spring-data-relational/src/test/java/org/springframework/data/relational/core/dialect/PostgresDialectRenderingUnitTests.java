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
 * Tests for {@link PostgresDialect}-specific rendering.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
public class PostgresDialectRenderingUnitTests {

	private final RenderContextFactory factory = new RenderContextFactory(PostgresDialect.INSTANCE);

	@BeforeEach
	public void before() throws Exception {
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

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithOffset() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).offset(10).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo OFFSET 10");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithLimitOffset() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).offset(20).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10 OFFSET 20");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo FOR UPDATE OF foo");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo FOR SHARE OF foo");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10 FOR UPDATE OF foo");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10 FOR SHARE OF foo");
	}

	@Test // GH-821
	void shouldRenderSelectOrderByWithNoOptions() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk())
				.from(table)
				.orderBy(OrderByField.from(Column.create("bar", table)))
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.bar");
	}

	@Test // GH-821
	void shouldRenderSelectOrderByWithDirection() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk())
				.from(table)
				.orderBy(OrderByField.from(Column.create("bar", table), Sort.Direction.ASC))
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.bar ASC");
	}

	@Test // GH-821
	void shouldRenderSelectOrderByWithNullPrecedence() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk())
				.from(table)
				.orderBy(OrderByField.from(Column.create("bar", table))
						.withNullHandling(Sort.NullHandling.NULLS_FIRST))
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.bar NULLS FIRST");
	}

	@Test // GH-821
	void shouldRenderSelectOrderByWithDirectionAndNullHandling() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk())
				.from(table)
				.orderBy(OrderByField.from(Column.create("bar", table), Sort.Direction.DESC)
						.withNullHandling(Sort.NullHandling.NULLS_FIRST))
				.build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo ORDER BY foo.bar DESC NULLS FIRST");
	}
}
