/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link Dialect} for HsqlDb.
 *
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Mikhail Polivakha
 */
public class HsqlDbDialect extends AbstractDialect {

	public static final HsqlDbDialect INSTANCE = new HsqlDbDialect();

	protected HsqlDbDialect() {}

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return AnsiDialect.LOCK_CLAUSE;
	}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset;
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return getOffset(offset) + " " + getLimit(limit);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	@Override
	public Collection<Object> getConverters() {
		Collection<Object> converters = new ArrayList<>(super.getConverters());
		converters.add(ZonedDateTimeOffsetDateTimeWritingConverter.INSTANCE);
		return converters;
	}

	/**
	 * Unfortunately, HSqlDb jdbc driver can accept {@link OffsetDateTime} as
	 * {@link java.sql.Types#TIMESTAMP_WITH_TIMEZONE}, but not {@link ZonedDateTime}
	 */
	@WritingConverter
	enum ZonedDateTimeOffsetDateTimeWritingConverter implements Converter<ZonedDateTime, OffsetDateTime> {

		INSTANCE;

		@Override
		public OffsetDateTime convert(ZonedDateTime source) {
			return source.toOffsetDateTime();
		}
	}
}
