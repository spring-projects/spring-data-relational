/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link JdbcColumnTypes}.
 *
 * @author Oliver Drotbohm
 * @author Christoph Strobl
 */
class JdbcColumnTypesUnitTests {

	@ParameterizedTest // GH-2184
	@ValueSource(classes = { Year.class })
	void usesIntegerFor(Class<?> type) {
		assertThat(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(type)).isEqualTo(Integer.class);
	}

	@ParameterizedTest // GH-2184
	@ValueSource(classes = { Month.class, MonthDay.class, YearMonth.class })
	void usesStringFor(Class<?> type) {
		assertThat(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(type)).isEqualTo(String.class);
	}

	@ParameterizedTest // GH-2184
	@ValueSource(classes = { OffsetTime.class, Instant.class })
	void usesTimestampFor(Class<?> type) {
		assertThat(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(type)).isEqualTo(Timestamp.class);
	}

}
