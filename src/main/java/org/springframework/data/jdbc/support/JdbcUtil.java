/*
 * Copyright 2017-2020 the original author or authors.
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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.support.JdbcUtils;

/**
 * Contains methods dealing with the quirks of JDBC, independent of any Entity, Aggregate or Repository abstraction.
 *
 * @author Jens Schauder
 */
@UtilityClass
public class JdbcUtil {

	private static final Map<Class<?>, Integer> sqlTypeMappings = new HashMap<>();

	static {

		sqlTypeMappings.put(String.class, Types.VARCHAR);
		sqlTypeMappings.put(BigInteger.class, Types.BIGINT);
		sqlTypeMappings.put(BigDecimal.class, Types.NUMERIC);
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

	public static int sqlTypeFor(Class<?> type) {
		return sqlTypeMappings.keySet().stream() //
				.filter(k -> k.isAssignableFrom(type)) //
				.findFirst() //
				.map(sqlTypeMappings::get) //
				.orElse(JdbcUtils.TYPE_UNKNOWN);
	}
}
