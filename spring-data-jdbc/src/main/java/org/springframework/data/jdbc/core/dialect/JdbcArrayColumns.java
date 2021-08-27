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

import org.springframework.data.relational.core.dialect.ArrayColumns;

/**
 * {@link org.springframework.data.relational.core.dialect.ArrayColumns} that offer JDBC specific functionality.
 * 
 * @author Jens Schauder
 * @since 2.3
 */
public interface JdbcArrayColumns extends ArrayColumns {

	JdbcArrayColumns UNSUPPORTED = new JdbcArrayColumns() {
		@Override
		public boolean isSupported() {
			return false;
		}

		@Override
		public Class<?> getArrayType(Class<?> userType) {
			throw new UnsupportedOperationException("Array types not supported");
		}

		@Override
		public String getSqlTypeRepresentation(JDBCType jdbcType) {
			throw new UnsupportedOperationException("Array types not supported");
		}
	};

	/**
	 * The appropriate SQL type as a String which should be used to represent the given {@link JDBCType} in an
	 * {@link java.sql.Array}. Defaults to the name of the argument.
	 * 
	 * @param jdbcType the {@link JDBCType} value representing the type that should be stored in the
	 *          {@link java.sql.Array}. Must not be {@literal null}.
	 * @return the appropriate SQL type as a String which should be used to represent the given {@link JDBCType} in an
	 *         {@link java.sql.Array}. Guaranteed to be not {@literal null}.
	 */
	default String getSqlTypeRepresentation(JDBCType jdbcType) {
		return jdbcType.getName();
	}

}
