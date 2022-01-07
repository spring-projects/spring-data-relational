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

import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.convert.Jsr310TimestampBasedConverters;
import org.springframework.data.relational.core.dialect.Db2Dialect;
import org.springframework.lang.NonNull;

/**
 * {@link Db2Dialect} that registers JDBC specific converters.
 *
 * @author Jens Schauder
 * @author Christoph Strobl
 * @author Mikhail Polivakha
 * @since 2.3
 */
public class JdbcDb2Dialect extends Db2Dialect {

	public static JdbcDb2Dialect INSTANCE = new JdbcDb2Dialect();

	protected JdbcDb2Dialect() {}

	@NonNull
	@Override
	public Collection<Object> getConverters() {
		List<Object> converters = new ArrayList<>(super.getConverters());
		converters.add(OffsetDateTimeToTimestampConverter.INSTANCE);
		converters.add(ZonedDateTimeToTimestampConverter.INSTANCE);
		converters.add(Jsr310TimestampBasedConverters.LocalDateTimeToTimestampConverter.INSTANCE);
		return converters;
	}

	/**
	 * Unfortunately, DB2 jdbc driver does not support Js310 (new Java date/time API) classes. Therefore for this
	 * dialect we need to convert these classes into {@link Timestamp}.
	 *
	 * @see OffsetDateTimeToTimestampConverter
	 * @see ZonedDateTimeToTimestampConverter
	 */
	@NonNull
    @Override
    public Map<Class<?>, Class<?>> getCustomJdbcColumnsMappings() {
		final Map<Class<?>, Class<?>> db2CustomJdbcColumnsMappings = super.getCustomJdbcColumnsMappings();
		db2CustomJdbcColumnsMappings.put(ZonedDateTime.class, Timestamp.class);
		db2CustomJdbcColumnsMappings.put(OffsetDateTime.class, Timestamp.class);
		return db2CustomJdbcColumnsMappings;
	}

	/**
	 * Unfortunately, DB2 jdbc driver does not support {@link Types#TIMESTAMP_WITH_TIMEZONE} as a type. Therefore for this
	 * dialect we need to use {@link Types#TIMESTAMP} and pass {@link Timestamp} instead of Jsr310 classes
	 *
	 * @see OffsetDateTimeToTimestampConverter
	 * @see ZonedDateTimeToTimestampConverter
	 */
	@NonNull
	@Override
	public Map<Class<?>, Integer> getCustomSqlCodesMappings() {
		final Map<Class<?>, Integer> db2CustomSqlCodesMappings = super.getCustomSqlCodesMappings();
		db2CustomSqlCodesMappings.put(ZonedDateTime.class, Types.TIMESTAMP);
		db2CustomSqlCodesMappings.put(OffsetDateTime.class, Types.TIMESTAMP);
		return db2CustomSqlCodesMappings;
	}

	/**
	 * {@link WritingConverter} from {@link OffsetDateTime} to {@link Timestamp}. The conversion preserves the
	 * {@link java.time.Instant} represented by {@link OffsetDateTime}
	 *
	 * @author Jens Schauder
	 * @since 2.3
	 */
	@WritingConverter
	enum OffsetDateTimeToTimestampConverter implements Converter<OffsetDateTime, Timestamp> {

		INSTANCE;

		@Override
		public Timestamp convert(OffsetDateTime source) {
			return Timestamp.from(source.toInstant());
		}
	}

	/**
	 * {@link WritingConverter} from {@link ZonedDateTime} to {@link Timestamp}. The conversion preserves the
	 * {@link java.time.Instant} represented by {@link ZonedDateTime}
	 *
	 * @author Mikhail Polivakha
	 * @since 3.0
	 */
	@WritingConverter
	enum ZonedDateTimeToTimestampConverter implements Converter<ZonedDateTime, Timestamp> {

		INSTANCE;

		@Override
		public Timestamp convert(ZonedDateTime source) {
			return Timestamp.from(source.toInstant());
		}
	}
}
