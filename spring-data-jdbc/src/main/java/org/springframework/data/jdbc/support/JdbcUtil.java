/*
 * Copyright 2017-2024 the original author or authors.
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
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.support.JdbcUtils;
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

		@Override
		public String toString() {
			return getName();
		}
	};
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
	 * Returns the {@link SQLType} value suitable for passing a value of the provided type to JDBC driver.
	 *
	 * @param type The type of value to be bound to a {@link java.sql.PreparedStatement}.
	 * @return a matching {@link SQLType} or {@link #TYPE_UNKNOWN}.
	 */
	public static SQLType targetSqlTypeFor(Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		return sqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(sqlTypeMappings::get) //
				.orElse(JdbcUtil.TYPE_UNKNOWN);
	}
}
