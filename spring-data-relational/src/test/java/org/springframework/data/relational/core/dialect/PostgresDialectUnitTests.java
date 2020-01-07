/*
 * Copyright 2019-2020 the original author or authors.
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

import org.junit.Test;

/**
 * Unit tests for {@link PostgresDialect}.
 *
 * @author Mark Paluch
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
}
