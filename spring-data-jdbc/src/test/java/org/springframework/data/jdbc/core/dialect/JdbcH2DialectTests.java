package org.springframework.data.jdbc.core.dialect;

import static org.assertj.core.api.Assertions.*;

import java.time.OffsetDateTime;

import org.h2.api.TimestampWithTimeZone;
import org.h2.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

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

/**
 * Tests for {@link JdbcH2Dialect}.
 * 
 * @author Jens Schauder
 */
class JdbcH2DialectTests {

	@Test
	void TimestampWithTimeZone2OffsetDateTimeConverterConvertsProperly() {

		JdbcH2Dialect.TimestampWithTimeZone2OffsetDateTimeConverter converter = JdbcH2Dialect.TimestampWithTimeZone2OffsetDateTimeConverter.INSTANCE;
		long dateValue = 123456789;
		long timeNanos = 987654321;
		int timeZoneOffsetSeconds = 4 * 60 * 60;
		TimestampWithTimeZone timestampWithTimeZone = new TimestampWithTimeZone(dateValue, timeNanos,
				timeZoneOffsetSeconds);

		OffsetDateTime offsetDateTime = converter.convert(timestampWithTimeZone);

		assertThat(offsetDateTime.getOffset().getTotalSeconds()).isEqualTo(timeZoneOffsetSeconds);
		assertThat(offsetDateTime.getNano()).isEqualTo(timeNanos);
		assertThat(offsetDateTime.toEpochSecond())
				.isEqualTo(DateTimeUtils.getEpochSeconds(dateValue, timeNanos, timeZoneOffsetSeconds));
	}
}
