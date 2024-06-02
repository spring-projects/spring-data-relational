package org.springframework.data.relational.core.dialect;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.relational.core.sql.LockOptions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;

/**
 * SQL Dialect for SQLite.
 *
 * @author Rudolf Schmidt
 * @since 3.3.0
 */
public class SQLiteDialect extends AbstractDialect {

	public static final SQLiteDialect INSTANCE = new SQLiteDialect();

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {
		@Override
		public String getLimit(long limit) {
			return String.format("limit %d", limit);
		}

		@Override
		public String getOffset(long offset) {
			throw new UnsupportedOperationException("offset alone not supported");
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("limit %d offset %d", limit, offset);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private static final LockClause LOCK_CLAUSE = new LockClause() {
		@Override
		public String getLock(LockOptions lockOptions) {
			return "";
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private SQLiteDialect() {
	}

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	@Override
	public Collection<Object> getConverters() {
		Collection<Object> converters = new ArrayList<>(super.getConverters());
		converters.add(LocalDateTimeToNumericConverter.INSTANCE);
		converters.add(NumericToLocalDateTimeConverter.INSTANCE);
		return converters;
	}

	@WritingConverter
	private enum LocalDateTimeToNumericConverter implements Converter<LocalDateTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalDateTime source) {
			return source.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
		}
	}

	@ReadingConverter
	private enum NumericToLocalDateTimeConverter implements Converter<Long, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(Long source) {
			return Instant.ofEpochMilli(source).atZone(ZoneOffset.UTC).toLocalDateTime();
		}
	}
}
