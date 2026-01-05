/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import static org.assertj.core.api.Assertions.*;

import microsoft.sql.DateTimeOffset;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;

/**
 * Tests for {@link JdbcSqlServerDialect}
 *
 * @author Mikhail Polivakha
 * @author Mark Paluch
 */
class JdbcSqlServerDialectTest {

	@Test // GH-1873
	void testCustomConversions() {

		JdbcCustomConversions conversions = JdbcCustomConversions.of(JdbcSqlServerDialect.INSTANCE, List.of());

		assertThat(conversions.hasCustomReadTarget(DateTimeOffset.class, Instant.class))
				.isTrue();
	}

	@Test // GH-2147
	void shouldReportSimpleTypes() {

		JdbcCustomConversions conversions = JdbcCustomConversions.of(JdbcSqlServerDialect.INSTANCE, List.of());

		assertThat(conversions.isSimpleType(DateTimeOffset.class)).isTrue();
		assertThat(conversions.getSimpleTypeHolder().isSimpleType(DateTimeOffset.class)).isTrue();
	}
}
