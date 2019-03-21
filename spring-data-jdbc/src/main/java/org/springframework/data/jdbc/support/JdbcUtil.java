/*
 * Copyright 2017-2019 the original author or authors.
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

import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
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
@UtilityClass
public class JdbcUtil {

	private static final Map<Class<?>, Integer> sqlTypeMappings = new HashMap<>();

	static {

		sqlTypeMappings.put(String.class, Types.VARCHAR);
		sqlTypeMappings.put(BigInteger.class, Types.BIGINT);
		sqlTypeMappings.put(BigDecimal.class, Types.DECIMAL);
		sqlTypeMappings.put(Byte.class, Types.TINYINT);
		sqlTypeMappings.put(byte.class, Types.TINYINT);
		sqlTypeMappings.put(Short.class, Types.SMALLINT);
		sqlTypeMappings.put(short.class, Types.SMALLINT);
		sqlTypeMappings.put(Integer.class, Types.INTEGER);
		sqlTypeMappings.put(int.class, Types.INTEGER);
		sqlTypeMappings.put(Long.class, Types.BIGINT);
		sqlTypeMappings.put(long.class, Types.BIGINT);
		sqlTypeMappings.put(Double.class, Types.DOUBLE);
		sqlTypeMappings.put(double.class, Types.DOUBLE);
		sqlTypeMappings.put(Float.class, Types.REAL);
		sqlTypeMappings.put(float.class, Types.REAL);
		sqlTypeMappings.put(Boolean.class, Types.BIT);
		sqlTypeMappings.put(boolean.class, Types.BIT);
		sqlTypeMappings.put(byte[].class, Types.VARBINARY);
		sqlTypeMappings.put(Date.class, Types.DATE);
		sqlTypeMappings.put(Time.class, Types.TIME);
		sqlTypeMappings.put(Timestamp.class, Types.TIMESTAMP);
	}

	/**
	 * Returns the {@link Types} value suitable for passing a value of the provided type to a
	 * {@link java.sql.PreparedStatement}.
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @return One of the values defined in {@link Types} or {@link JdbcUtils#TYPE_UNKNOWN}.
	 */
	public static int sqlTypeFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null.");

		return sqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(sqlTypeMappings::get) //
				.orElse(JdbcUtils.TYPE_UNKNOWN);
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
	 */
	@Nullable
	public static JDBCType jdbcTypeFor(Class<?> type) {
		return jdbcTypeFor(sqlTypeFor(type));
	}
}
