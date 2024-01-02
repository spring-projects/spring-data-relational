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
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.Table;

import java.util.Collections;

/**
 * Unit tests for {@link PostgresDialect}.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 */
public class PostgresDialectUnitTests {

	@Test // DATAJDBC-278
	public void shouldSupportArrays() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isTrue();
	}

	@Test // DATAJDBC-278
	public void shouldUseBoxedArrayTypesForPrimitiveTypes() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertSoftly(it -> {
			it.assertThat(arrayColumns.getArrayType(int.class)).isEqualTo(Integer.class);
			it.assertThat(arrayColumns.getArrayType(double.class)).isEqualTo(Double.class);
			it.assertThat(arrayColumns.getArrayType(String.class)).isEqualTo(String.class);
		});
	}

	@Test // DATAJDBC-278
	public void shouldRenderLimit() {

		LimitClause limit = PostgresDialect.INSTANCE.limit();

		assertThat(limit.getClausePosition()).isEqualTo(LimitClause.Position.AFTER_ORDER_BY);
		assertThat(limit.getLimit(10)).isEqualTo("LIMIT 10");
	}

	@Test // DATAJDBC-278
	public void shouldRenderOffset() {

		LimitClause limit = PostgresDialect.INSTANCE.limit();

		assertThat(limit.getOffset(10)).isEqualTo("OFFSET 10");
	}

	@Test // DATAJDBC-278
	public void shouldRenderLimitOffset() {

		LimitClause limit = PostgresDialect.INSTANCE.limit();

		assertThat(limit.getLimitOffset(20, 10)).isEqualTo("LIMIT 20 OFFSET 10");
	}

	@Test // DATAJDBC-498
	public void shouldRenderLock() {

		LockClause lock = PostgresDialect.INSTANCE.lock();
		From from = mock(From.class);
		when(from.getTables()).thenReturn(Collections.singletonList(Table.create("dummy_table")));

		assertThat(lock.getLock(new LockOptions(LockMode.PESSIMISTIC_WRITE, from))).isEqualTo("FOR UPDATE OF dummy_table");
		assertThat(lock.getLock(new LockOptions(LockMode.PESSIMISTIC_READ, from))).isEqualTo("FOR SHARE OF dummy_table");
		assertThat(lock.getClausePosition()).isEqualTo(LockClause.Position.AFTER_ORDER_BY);
	}
}
