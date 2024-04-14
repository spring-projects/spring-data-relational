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

	private SQLiteDialect() {
	}

	@Override
	public LimitClause limit() {
		return new LimitClause() {
			@Override
			public String getLimit(final long limit) {
				return String.format("limit %d", limit);
			}

			@Override
			public String getOffset(final long offset) {
				throw new UnsupportedOperationException("offset alone not supported");
			}

			@Override
			public String getLimitOffset(final long limit, final long offset) {
				return String.format("limit %d offset %d", limit, offset);
			}

			@Override
			public Position getClausePosition() {
				return Position.AFTER_ORDER_BY;
			}
		};
	}

	@Override
	public LockClause lock() {
		return new LockClause() {
			@Override
			public String getLock(final LockOptions lockOptions) {
				return "with lock";
			}

			@Override
			public Position getClausePosition() {
				return Position.AFTER_ORDER_BY;
			}
		};
	}

	@Override
	public Collection<Object> getConverters() {
		final var converters = new ArrayList<>(super.getConverters());
		converters.add(LocalDateTimeToNumericConverter.INSTANCE);
		converters.add(NumericToLocalDateTimeConverter.INSTANCE);
		return converters;
	}

	@WritingConverter
	private enum LocalDateTimeToNumericConverter implements Converter<LocalDateTime, Long> {

		INSTANCE;

		@Override
		public Long convert(final LocalDateTime source) {
			return source.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
		}
	}

	@ReadingConverter
	private enum NumericToLocalDateTimeConverter implements Converter<Long, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(final Long source) {
			return Instant.ofEpochMilli(source).atZone(ZoneOffset.UTC).toLocalDateTime();
		}
	}
}
