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

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.LockOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MySqlDialect}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 */
public class MySqlDialectUnitTests {

	@Test // DATAJDBC-278
	public void shouldNotSupportArrays() {

		ArrayColumns arrayColumns = MySqlDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isFalse();
	}

	@Test // DATAJDBC-278
	public void shouldRenderLimit() {

		LimitClause limit = MySqlDialect.INSTANCE.limit();

		assertThat(limit.getClausePosition()).isEqualTo(LimitClause.Position.AFTER_ORDER_BY);
		assertThat(limit.getLimit(10)).isEqualTo("LIMIT 10");
	}

	@Test // DATAJDBC-278
	public void shouldRenderOffset() {

		LimitClause limit = MySqlDialect.INSTANCE.limit();

		assertThat(limit.getOffset(10)).isEqualTo("LIMIT 10, 18446744073709551615");
	}

	@Test // DATAJDBC-278
	public void shouldRenderLimitOffset() {

		LimitClause limit = MySqlDialect.INSTANCE.limit();

		assertThat(limit.getLimitOffset(20, 10)).isEqualTo("LIMIT 10, 20");
	}

	@Test // DATAJDBC-386
	public void shouldQuoteIdentifiersUsingBackticks() {

		String abcQuoted = MySqlDialect.INSTANCE.getIdentifierProcessing().quote("abc");

		assertThat(abcQuoted).isEqualTo("`abc`");
	}

	@Test // DATAJDBC-498
	public void shouldRenderLock() {

		LockClause lock = MySqlDialect.INSTANCE.lock();
		From from = mock(From.class);

		assertThat(lock.getLock(new LockOptions(LockMode.PESSIMISTIC_WRITE, from))).isEqualTo("FOR UPDATE");
		assertThat(lock.getLock(new LockOptions(LockMode.PESSIMISTIC_READ, from))).isEqualTo("LOCK IN SHARE MODE");
		assertThat(lock.getClausePosition()).isEqualTo(LockClause.Position.AFTER_ORDER_BY);
	}
}
