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
package org.springframework.data.jdbc.core.dialect;

import microsoft.sql.DateTimeOffset;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.relational.core.dialect.SqlServerDialect;

/**
 * {@link SqlServerDialect} that registers JDBC specific converters.
 *
 * @author Jens Schauder
 * @author Christoph Strobl
 * @author Mikhail Polivakha
 * @since 2.3
 */
public class JdbcSqlServerDialect extends SqlServerDialect {

	public static JdbcSqlServerDialect INSTANCE = new JdbcSqlServerDialect();

	@Override
	public Collection<Object> getConverters() {

		List<Object> converters = new ArrayList<>(super.getConverters());
		converters.add(DateTimeOffsetToOffsetDateTimeConverter.INSTANCE);
		converters.add(DateTimeOffsetToInstantConverter.INSTANCE);
		return converters;
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
