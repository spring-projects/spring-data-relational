/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.jdbc.support;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Contains methods dealing with the quirks of JDBC, independent of any Entity, Aggregate or Repository abstraction.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Mikhail Polivakha
 */
public final class JdbcUtil {

	private static final Map<Class<?>, Integer> commonSqlTypeMappings = new HashMap<>();

	static {

		commonSqlTypeMappings.put(String.class, Types.VARCHAR);
		commonSqlTypeMappings.put(BigInteger.class, Types.BIGINT);
		commonSqlTypeMappings.put(BigDecimal.class, Types.DECIMAL);
		commonSqlTypeMappings.put(Byte.class, Types.TINYINT);
		commonSqlTypeMappings.put(byte.class, Types.TINYINT);
		commonSqlTypeMappings.put(Short.class, Types.SMALLINT);
		commonSqlTypeMappings.put(short.class, Types.SMALLINT);
		commonSqlTypeMappings.put(Integer.class, Types.INTEGER);
		commonSqlTypeMappings.put(int.class, Types.INTEGER);
		commonSqlTypeMappings.put(Long.class, Types.BIGINT);
		commonSqlTypeMappings.put(long.class, Types.BIGINT);
		commonSqlTypeMappings.put(Double.class, Types.DOUBLE);
		commonSqlTypeMappings.put(double.class, Types.DOUBLE);
		commonSqlTypeMappings.put(Float.class, Types.REAL);
		commonSqlTypeMappings.put(float.class, Types.REAL);
		commonSqlTypeMappings.put(Boolean.class, Types.BIT);
		commonSqlTypeMappings.put(boolean.class, Types.BIT);
		commonSqlTypeMappings.put(byte[].class, Types.VARBINARY);
		commonSqlTypeMappings.put(Date.class, Types.DATE);
		commonSqlTypeMappings.put(Time.class, Types.TIME);
		commonSqlTypeMappings.put(Timestamp.class, Types.TIMESTAMP);
		commonSqlTypeMappings.put(OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE);
		commonSqlTypeMappings.put(ZonedDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE);
	}

	private JdbcUtil() {
		throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
	}

	/**
	 * Returns the {@link Types} value suitable for passing a value of the provided type to a
	 * {@link java.sql.PreparedStatement}.
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @return One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 * @deprecated because this method will make lookup only in common sql types map and therefore
	 * 			   will not take into account dialect specific sql types codes mappings. When possible,
	 * 			   please, use {@link #sqlTypeFor(Class, Dialect)}
	 */
	@Deprecated
	public static int sqlTypeFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null.");

		return commonSqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(commonSqlTypeMappings::get) //
				.orElse(JdbcUtils.TYPE_UNKNOWN);
	}

	/**
	 * Returns the {@link Types} value suitable for passing a value of the provided type to a
	 * {@link java.sql.PreparedStatement} giving regards to passed dialect specific sql codes
	 * mappings
	 *
	 * The main motivation for this method is, in general, the fact that we cannot assign the same int code for the same java class for
	 * each dialect. For example, currently, there MySQL Driver cannot handle {@link Types#TIMESTAMP_WITH_TIMEZONE}
	 * to be set by the means of {@link java.sql.PreparedStatement#setObject(int, Object, int)}. If we We need to instead set
	 * it be the mean
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @param dialect represents the dialect, in the context of which the int sql code must be derived
	 * @return the int code of sql type in regards to custom dialect mappings
	 *
	 * @see Types
	 */
	public static int sqlTypeFor(Class<?> type, Dialect dialect) {

		Assert.notNull(type, "Type must not be null.");

		return Optional
				.ofNullable(dialect.getCustomSqlCodesMappings().get(type))
				.orElse(commonSqlTypeMappings.keySet().stream()
						.filter(k -> k.isAssignableFrom(type))
						.findFirst()
						.map(commonSqlTypeMappings::get)
						.orElse(JdbcUtils.TYPE_UNKNOWN)
				);
	}

	/**
	 * Converts a {@link JDBCType} to an {@code int} value as defined in {@link Types}.
	 *
	 * @param jdbcType value to be converted. May be {@literal null}.
	 * @return One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 */
	public static int sqlTypeFor(@Nullable JDBCType jdbcType) {
		return jdbcType == null ? JdbcUtils.TYPE_UNKNOWN : jdbcType.getVendorTypeNumber();
	}

	/**
	 * Converts a value defined in {@link Types} into a {@link JDBCType} instance or {@literal null} if the value is
	 * {@link JdbcUtils#TYPE_UNKNOWN}
	 *
	 * @param sqlType One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 * @return a matching {@link JDBCType} instance or {@literal null}.
	 */
	@Nullable
	public static JDBCType jdbcTypeFor(int sqlType) {

		if (sqlType == JdbcUtils.TYPE_UNKNOWN) {
			return null;
		}

		return JDBCType.valueOf(sqlType);
	}

	/**
	 * Returns the {@link JDBCType} suitable for passing a value of the provided type to a
	 * {@link java.sql.PreparedStatement}.
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @return a matching {@link JDBCType} instance or {@literal null}.
	 * @deprecated because this method will make lookup only in common sql types map and therefore
	 * 			   will not take into account dialect specific sql types codes mappings. When possible,
	 * 			   please, use {@link #jdbcTypeFor(Class, Dialect)}
	 */
	@Nullable
	@Deprecated
	public static JDBCType jdbcTypeFor(Class<?> type) {
		return jdbcTypeFor(sqlTypeFor(type));
	}


	/**
	 * Returns the {@link JDBCType} suitable for passing a value of the provided type to a
	 * {@link java.sql.PreparedStatement} giving regards to passed dialect specific sql codes
	 * mappings
	 *
	 * The main motivation for this method is, in general, the fact that we cannot assign the same int code for the same java class for
	 * each dialect. For example, currently, there MySQL Driver cannot handle {@link Types#TIMESTAMP_WITH_TIMEZONE}
	 * to be set by the means of {@link java.sql.PreparedStatement#setObject(int, Object, int)}. If we We need to instead set
	 * it be the mean
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @param dialect represents the dialect, in the context of which the int sql code must be derived
	 * @return a matching {@link JDBCType} instance or {@literal null}.
	 */
	@Nullable
	public static JDBCType jdbcTypeFor(Class<?> type, Dialect dialect) {
		return jdbcTypeFor(sqlTypeFor(type, dialect));
	}
}
