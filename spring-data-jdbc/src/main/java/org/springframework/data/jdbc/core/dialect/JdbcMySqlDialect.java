/*
 * Copyright 2021-2025 the original author or authors.
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

import static java.time.ZoneId.*;

import java.sql.JDBCType;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.relational.core.dialect.MySqlDialect;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.lang.NonNull;

/**
 * {@link MySqlDialect} that registers JDBC specific converters.
 *
 * @author Jens Schauder
 * @author Christoph Strobl
 * @author Mikhail Polivakha
 * @since 2.3
 */
public class JdbcMySqlDialect extends MySqlDialect implements JdbcDialect {

	/**
	 * Predefined instance of the {@literal JdbcMySqlDialect}.
	 *
	 * @deprecated Use the constructor instead. There is no one correct MySqlDialect, since the behaviour of MySql depends
	 *             on various configuration options. See
	 * 
	 *             <pre>
	 * <a href="https://dev.mysql.com/doc/refman/8.4/en/identifier-case-sensitivity.html">Identifier Case Sensitivity</a>
	 *             </pre>
	 */
	@Deprecated(forRemoval = true, since = "4.0") public static final JdbcMySqlDialect INSTANCE = new JdbcMySqlDialect();

	public JdbcMySqlDialect(IdentifierProcessing identifierProcessing) {
		super(identifierProcessing);
	}

	protected JdbcMySqlDialect() {}

	@Override
	public Collection<Object> getConverters() {

		ArrayList<Object> converters = new ArrayList<>(super.getConverters());
		converters.add(OffsetDateTimeToTimestampJdbcValueConverter.INSTANCE);
		converters.add(LocalDateTimeToDateConverter.INSTANCE);

		return converters;
	}

	@WritingConverter
	enum OffsetDateTimeToTimestampJdbcValueConverter implements Converter<OffsetDateTime, JdbcValue> {

		INSTANCE;

		@Override
		public JdbcValue convert(OffsetDateTime source) {
			return JdbcValue.of(source, JDBCType.TIMESTAMP);
		}
	}

	@ReadingConverter
	enum LocalDateTimeToDateConverter implements Converter<LocalDateTime, Date> {

		INSTANCE;

		@NonNull
		@Override
		public Date convert(LocalDateTime source) {
			return Date.from(source.atZone(systemDefault()).toInstant());
		}
	}
}
