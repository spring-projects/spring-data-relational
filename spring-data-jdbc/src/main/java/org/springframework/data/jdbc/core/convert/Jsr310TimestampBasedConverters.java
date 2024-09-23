/*
 * Copyright 2020-2024 the original author or authors.
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

import static java.time.LocalDateTime.*;
import static java.time.ZoneId.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.lang.NonNull;

/**
 * Helper class to register JSR-310 specific {@link Converter} implementations. These converters are based on
 * {@link java.sql.Timestamp} instead of {@link Date} and therefore preserve nanosecond precision
 *
 * @see org.springframework.data.convert.Jsr310Converters
 * @author Jens Schauder
 * @since 2.2
 */
public abstract class Jsr310TimestampBasedConverters {

	/**
	 * Returns the converters to be registered.
	 *
	 * Note that the {@link LocalDateTimeToTimestampConverter} is not included, since many database don't need that conversion.
	 * Databases that do need it, should include it in the conversions offered by their respective dialect.
	 *
	 * @return a collection of converters. Guaranteed to be not {@literal null}.
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		List<Converter<?, ?>> converters = new ArrayList<>(8);

		converters.add(TimestampToLocalDateTimeConverter.INSTANCE);
		converters.add(TimestampToLocalDateConverter.INSTANCE);
		converters.add(LocalDateToTimestampConverter.INSTANCE);
		converters.add(TimestampToLocalTimeConverter.INSTANCE);
		converters.add(LocalTimeToTimestampConverter.INSTANCE);
		converters.add(TimestampToInstantConverter.INSTANCE);
		converters.add(InstantToTimestampConverter.INSTANCE);

		return converters;
	}

	@ReadingConverter
	public enum TimestampToLocalDateTimeConverter implements Converter<Timestamp, LocalDateTime> {

		INSTANCE;

		@NonNull
		@Override
		public LocalDateTime convert(Timestamp source) {
			return ofInstant(source.toInstant(), systemDefault());
		}
	}

	@WritingConverter
	public enum LocalDateTimeToTimestampConverter implements Converter<LocalDateTime, Timestamp> {

		INSTANCE;

		@NonNull
		@Override
		public Timestamp convert(LocalDateTime source) {
			return Timestamp.from(source.atZone(systemDefault()).toInstant());
		}
	}

	@ReadingConverter
	public enum TimestampToLocalDateConverter implements Converter<Timestamp, LocalDate> {

		INSTANCE;

		@NonNull
		@Override
		public LocalDate convert(Timestamp source) {
			return source.toLocalDateTime().toLocalDate();
		}
	}

	@WritingConverter
	public enum LocalDateToTimestampConverter implements Converter<LocalDate, Timestamp> {

		INSTANCE;

		@NonNull
		@Override
		public Timestamp convert(LocalDate source) {
			return Timestamp.from(source.atStartOfDay(systemDefault()).toInstant());
		}
	}

	@ReadingConverter
	public enum TimestampToLocalTimeConverter implements Converter<Timestamp, LocalTime> {

		INSTANCE;

		@NonNull
		@Override
		public LocalTime convert(Timestamp source) {
			return source.toLocalDateTime().toLocalTime();
		}
	}

	@WritingConverter
	public enum LocalTimeToTimestampConverter implements Converter<LocalTime, Timestamp> {

		INSTANCE;

		@NonNull
		@Override
		public Timestamp convert(LocalTime source) {
			return Timestamp.from(source.atDate(LocalDate.now()).atZone(systemDefault()).toInstant());
		}
	}

	@ReadingConverter
	public enum TimestampToInstantConverter implements Converter<Timestamp, Instant> {

		INSTANCE;

		@NonNull
		@Override
		public Instant convert(Timestamp source) {
			return source.toInstant();
		}
	}

	@WritingConverter
	public enum InstantToTimestampConverter implements Converter<Instant, Timestamp> {

		INSTANCE;

		@NonNull
		@Override
		public Timestamp convert(Instant source) {
			return Timestamp.from(source);
		}
	}
}
