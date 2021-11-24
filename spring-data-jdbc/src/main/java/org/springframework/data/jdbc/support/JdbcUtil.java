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
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Contains methods dealing with the quirks of JDBC, independent of any Entity, Aggregate or Repository abstraction.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 */
public final class JdbcUtil {

	public static final SQLType TYPE_UNKNOWN = new SQLType() {
		@Override
		public String getName() {
			return "UNKNOWN";
		}

		@Override
		public String getVendor() {
			return "Spring";
		}

		@Override
		public Integer getVendorTypeNumber() {
			return JdbcUtils.TYPE_UNKNOWN;
		}
	} ;
	private static final Map<Class<?>, SQLType> sqlTypeMappings = new HashMap<>();

	static {

		sqlTypeMappings.put(String.class, JDBCType.VARCHAR);
		sqlTypeMappings.put(BigInteger.class, JDBCType.BIGINT);
		sqlTypeMappings.put(BigDecimal.class, JDBCType.DECIMAL);
		sqlTypeMappings.put(Byte.class, JDBCType.TINYINT);
		sqlTypeMappings.put(byte.class, JDBCType.TINYINT);
		sqlTypeMappings.put(Short.class, JDBCType.SMALLINT);
		sqlTypeMappings.put(short.class, JDBCType.SMALLINT);
		sqlTypeMappings.put(Integer.class, JDBCType.INTEGER);
		sqlTypeMappings.put(int.class, JDBCType.INTEGER);
		sqlTypeMappings.put(Long.class, JDBCType.BIGINT);
		sqlTypeMappings.put(long.class, JDBCType.BIGINT);
		sqlTypeMappings.put(Double.class, JDBCType.DOUBLE);
		sqlTypeMappings.put(double.class, JDBCType.DOUBLE);
		sqlTypeMappings.put(Float.class, JDBCType.REAL);
		sqlTypeMappings.put(float.class, JDBCType.REAL);
		sqlTypeMappings.put(Boolean.class, JDBCType.BIT);
		sqlTypeMappings.put(boolean.class, JDBCType.BIT);
		sqlTypeMappings.put(byte[].class, JDBCType.VARBINARY);
		sqlTypeMappings.put(Date.class, JDBCType.DATE);
		sqlTypeMappings.put(Time.class, JDBCType.TIME);
		sqlTypeMappings.put(Timestamp.class, JDBCType.TIMESTAMP);
		sqlTypeMappings.put(OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE);
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
	 * @deprecated use {@link #targetSqlTypeFor(Class)} instead.
	 */
	@Deprecated
	public static int sqlTypeFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null.");

		return sqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(sqlTypeMappings::get) //
				.map(SQLType::getVendorTypeNumber)
				.orElse(JdbcUtils.TYPE_UNKNOWN);
	}

	/**
	 * Returns the {@link SQLType} value suitable for passing a value of the provided type to JDBC driver.
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @return a matching {@link SQLType} or {@link #TYPE_UNKNOWN}.
	 */
	public static SQLType targetSqlTypeFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null.");

		return sqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(sqlTypeMappings::get) //
				.orElse(JdbcUtil.TYPE_UNKNOWN);
	}

	/**
	 * Converts a {@link JDBCType} to an {@code int} value as defined in {@link Types}.
	 *
	 * @param jdbcType value to be converted. May be {@literal null}.
	 * @return One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 * @deprecated there is no replacement.
	 */
	@Deprecated
	public static int sqlTypeFor(@Nullable SQLType jdbcType) {
		return jdbcType == null ? JdbcUtils.TYPE_UNKNOWN : jdbcType.getVendorTypeNumber();
	}

	/**
	 * Converts a value defined in {@link Types} into a {@link JDBCType} instance or {@literal null} if the value is
	 * {@link JdbcUtils#TYPE_UNKNOWN}
	 *
	 * @param sqlType One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 * @return a matching {@link JDBCType} instance or {@literal null}.
	 * @deprecated This is now a noop
	 */
	@Nullable
	@Deprecated
	public static SQLType jdbcTypeFor(int sqlType) {

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
	 * @deprecated Use {@link #targetSqlTypeFor(Class)} instead.
	 */
	@Deprecated
	public static SQLType jdbcTypeFor(Class<?> type) {

		return targetSqlTypeFor(type);
	}
}
