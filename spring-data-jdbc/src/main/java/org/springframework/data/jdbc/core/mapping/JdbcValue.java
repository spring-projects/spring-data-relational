/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.Objects;

import org.springframework.lang.Nullable;

/**
 * Wraps a value with the JDBCType that should be used to pass it as a bind parameter to a
 * {@link java.sql.PreparedStatement}. Register a converter from any type to {@link JdbcValue} in order to control the
 * value and the {@link JDBCType} as which a value should get passed to the JDBC driver.
 *
 * @author Jens Schauder
 * @since 2.4
 */
public class JdbcValue {

	private final Object value;
	private final SQLType jdbcType;

	protected JdbcValue(@Nullable Object value, @Nullable SQLType jdbcType) {

		this.value = value;
		this.jdbcType = jdbcType;
	}

	public static JdbcValue of(@Nullable Object value, @Nullable SQLType jdbcType) {
		return new JdbcValue(value, jdbcType);
	}

	@Nullable
	public Object getValue() {
		return this.value;
	}

	@Nullable
	public SQLType getJdbcType() {
		return this.jdbcType;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		JdbcValue jdbcValue = (JdbcValue) o;
		return Objects.equals(value, jdbcValue.value) && jdbcType == jdbcValue.jdbcType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, jdbcType);
	}
}
