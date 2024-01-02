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

import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Tests for {@link MySqlDialect}-specific rendering.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 */
public class MySqlDialectRenderingUnitTests {

	private final RenderContextFactory factory = new RenderContextFactory(MySqlDialect.INSTANCE);

	@BeforeEach
	public void before() {
		factory.setNamingStrategy(NamingStrategies.asIs());
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

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10, 18446744073709551615");
	}

	@Test // DATAJDBC-278
	public void shouldRenderSelectWithLimitOffset() {

		Table table = Table.create("foo");
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).offset(20).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 20, 10");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo FOR UPDATE");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LOCK IN SHARE MODE");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitWithLockWrite() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10 FOR UPDATE");
	}

	@Test // DATAJDBC-498
	public void shouldRenderSelectWithLimitWithLockRead() {

		Table table = Table.create("foo");
		LockMode lockMode = LockMode.PESSIMISTIC_READ;
		Select select = StatementBuilder.select(table.asterisk()).from(table).limit(10).lock(lockMode).build();

		String sql = SqlRenderer.create(factory.createRenderContext()).render(select);

		assertThat(sql).isEqualTo("SELECT foo.* FROM foo LIMIT 10 LOCK IN SHARE MODE");
	}
}
