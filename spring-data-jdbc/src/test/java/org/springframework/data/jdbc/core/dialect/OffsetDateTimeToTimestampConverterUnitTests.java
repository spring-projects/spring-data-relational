package org.springframework.data.jdbc.core.dialect;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.*;

/*
 * Copyright 2021-2024 the original author or authors.
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

/**
 * Tests for {@link JdbcDb2Dialect.OffsetDateTimeToTimestampConverter}.
 *
 * @author Jens Schauder
 */
class OffsetDateTimeToTimestampConverterUnitTests {

	@Test
	void conversionPreservesInstant() {

		OffsetDateTime offsetDateTime = OffsetDateTime.of(5, 5, 5, 5,5,5,123456789, ZoneOffset.ofHours(3));

		Timestamp timestamp = JdbcDb2Dialect.OffsetDateTimeToTimestampConverter.INSTANCE.convert(offsetDateTime);

		assertThat(timestamp.toInstant()).isEqualTo(offsetDateTime.toInstant());
	}
}
