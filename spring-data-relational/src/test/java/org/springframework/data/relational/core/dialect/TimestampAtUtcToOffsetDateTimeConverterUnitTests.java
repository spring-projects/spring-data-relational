package org.springframework.data.relational.core.dialect;

import static org.assertj.core.api.Assertions.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;

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
 * Tests {@link TimestampAtUtcToOffsetDateTimeConverter}.
 *
 * @author Jens Schauder
 */
class TimestampAtUtcToOffsetDateTimeConverterUnitTests {

	@Test
	void conversionMaintainsInstant() {

		Timestamp timestamp = Timestamp.from(Instant.now());
		OffsetDateTime converted = TimestampAtUtcToOffsetDateTimeConverter.INSTANCE.convert(timestamp);

		assertThat(converted.toInstant()).isEqualTo(timestamp.toInstant());
	}
}
