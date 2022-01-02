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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.h2.api.TimestampWithTimeZone;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

/**
 * Converter converting from an H2 internal representation of a timestamp with time zone to an OffsetDateTime.
 *
 * Only required for H2 versions < 2.0
 * 
 * @author Jens Schauder
 * @since 2.7
 */
@ReadingConverter
public enum H2TimestampWithTimeZoneToOffsetDateTimeConverter implements Converter<TimestampWithTimeZone, OffsetDateTime> {

	INSTANCE;

	@Override
	public OffsetDateTime convert(TimestampWithTimeZone source) {

		long nanosInSecond = 1_000_000_000;
		long nanosInMinute = nanosInSecond * 60;
		long nanosInHour = nanosInMinute * 60;

		long hours = (source.getNanosSinceMidnight() / nanosInHour);

		long nanosInHours = hours * nanosInHour;
		long nanosLeft = source.getNanosSinceMidnight() - nanosInHours;
		long minutes = nanosLeft / nanosInMinute;

		long nanosInMinutes = minutes * nanosInMinute;
		nanosLeft -= nanosInMinutes;
		long seconds = nanosLeft / nanosInSecond;

		long nanosInSeconds = seconds * nanosInSecond;
		nanosLeft -= nanosInSeconds;
		ZoneOffset offset = ZoneOffset.ofTotalSeconds(source.getTimeZoneOffsetSeconds());

		return OffsetDateTime.of(source.getYear(), source.getMonth(), source.getDay(), (int) hours, (int) minutes,
				(int) seconds, (int) nanosLeft, offset);
	}
}
