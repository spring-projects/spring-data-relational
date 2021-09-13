/*
 * Copyright 2021 the original author or authors.
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

import java.sql.JDBCType;

import org.springframework.data.relational.core.dialect.PostgresDialect;

/**
 * JDBC specific Postgres Dialect.
 * 
 * @author Jens Schauder
 * @since 2.3
 */
public class JdbcPostgresDialect extends PostgresDialect implements JdbcDialect {

	public static final JdbcPostgresDialect INSTANCE = new JdbcPostgresDialect();
	private static final JdbcPostgresArrayColumns ARRAY_COLUMNS = new JdbcPostgresArrayColumns();

	@Override
	public JdbcArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	static class JdbcPostgresArrayColumns extends PostgresArrayColumns implements JdbcArrayColumns {
		@Override
		public String getSqlTypeRepresentation(JDBCType jdbcType) {

			if (jdbcType == JDBCType.DOUBLE) {
				return "FLOAT8";
			}
			if (jdbcType == JDBCType.REAL) {
				return "FLOAT4";
			}

			return jdbcType.getName();
		}
	}
}
