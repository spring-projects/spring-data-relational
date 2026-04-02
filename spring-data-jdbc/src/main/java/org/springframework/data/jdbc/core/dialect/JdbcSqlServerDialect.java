/*
 * Copyright 2021-present the original author or authors.
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

import microsoft.sql.DateTimeOffset;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.relational.core.dialect.SqlServerDialect;
import org.springframework.data.util.ClassUtils;

/**
 * {@link SqlServerDialect} that registers JDBC specific converters.
 *
 * @author Jens Schauder
 * @author Christoph Strobl
 * @author Mikhail Polivakha
 * @author Mark Paluch
 * @since 2.3
 */
public class JdbcSqlServerDialect extends SqlServerDialect implements JdbcDialect {

	public static final JdbcSqlServerDialect INSTANCE = new JdbcSqlServerDialect();

	private static final @Nullable Class<?> DATE_TIME_OFFSET_CLASS = ClassUtils
			.loadIfPresent("microsoft.sql.DateTimeOffset", JdbcSqlServerDialect.class.getClassLoader());

	private static final Set<Class<?>> SIMPLE_TYPES = DATE_TIME_OFFSET_CLASS != null ? Set.of(DATE_TIME_OFFSET_CLASS)
			: Collections.emptySet();

	private static final List<Object> CONVERTERS = DATE_TIME_OFFSET_CLASS != null
			? List.of(DateTimeOffsetToOffsetDateTimeConverter.INSTANCE, DateTimeOffsetToInstantConverter.INSTANCE)
			: Collections.emptyList();

	@Override
	public Collection<Object> getConverters() {
		return CONVERTERS;
	}

	@Override
	public Set<Class<?>> simpleTypes() {
		return SIMPLE_TYPES;
	}

	@ReadingConverter
	enum DateTimeOffsetToOffsetDateTimeConverter implements Converter<DateTimeOffset, OffsetDateTime> {

		INSTANCE;

		@Override
		public OffsetDateTime convert(DateTimeOffset source) {
			return source.getOffsetDateTime();
		}

	}

	@ReadingConverter
	enum DateTimeOffsetToInstantConverter implements Converter<DateTimeOffset, Instant> {

		INSTANCE;

		@Override
		public Instant convert(DateTimeOffset source) {
			return source.getOffsetDateTime().toInstant();
		}

	}

}
