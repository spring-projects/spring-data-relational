/*
 * Copyright 2021 the original author or authors.
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

import static org.assertj.core.api.SoftAssertions.*;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcValue;

/**
 * Tests for {@link JdbcMySqlDialect}.
 *
 * @author Jens Schauder
 */
class JdbcMySqlDialectUnitTests {

	@Test // GH-974
	void testCustomConversions() {

		JdbcCustomConversions customConversions = new JdbcCustomConversions(
				(List<?>) new JdbcMySqlDialect().getConverters());

		assertSoftly(softly -> {

			softly.assertThat(customConversions.getCustomWriteTarget(LocalDateTime.class)).isEmpty();
			softly.assertThat(customConversions.getCustomWriteTarget(OffsetDateTime.class)).contains(JdbcValue.class);
		});
	}
}
