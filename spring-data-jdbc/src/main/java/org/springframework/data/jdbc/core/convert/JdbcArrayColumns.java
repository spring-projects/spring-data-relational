/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.sql.SQLType;

import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.dialect.ArrayColumns;

/**
 * {@link org.springframework.data.relational.core.dialect.ArrayColumns} that offer JDBC-specific functionality.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.3
 */
public interface JdbcArrayColumns extends ArrayColumns {

	@Override
	default Class<?> getArrayType(Class<?> userType) {
		return ArrayColumns.unwrapComponentType(userType);
	}

	/**
	 * Determine the {@link SQLType} for a given {@link Class array component type}.
	 *
	 * @param componentType component type of the array.
	 * @return the dialect-supported array type.
	 * @since 3.1.3
	 */
	default SQLType getSqlType(Class<?> componentType) {
		return JdbcUtil.targetSqlTypeFor(getArrayType(componentType));
	}

	/**
	 * The appropriate SQL type as a String which should be used to represent the given {@link SQLType} in an
	 * {@link java.sql.Array}. Defaults to the name of the argument.
	 *
	 * @param jdbcType the {@link SQLType} value representing the type that should be stored in the
	 *          {@link java.sql.Array}. Must not be {@literal null}.
	 * @return the appropriate SQL type as a String which should be used to represent the given {@link SQLType} in an
	 *         {@link java.sql.Array}. Guaranteed to be not {@literal null}.
	 */
	default String getArrayTypeName(SQLType jdbcType) {
		return jdbcType.getName();
	}

	/**
	 * Default {@link ArrayColumns} implementation for dialects that do not support array-typed columns.
	 */
	enum Unsupported implements JdbcArrayColumns {

		INSTANCE;

		@Override
		public boolean isSupported() {
			return false;
		}

		@Override
		public String getArrayTypeName(SQLType jdbcType) {
			throw new UnsupportedOperationException("Array types not supported");
		}

	}

	/**
	 * Default {@link ArrayColumns} implementation for dialects that do not support array-typed columns.
	 */
	enum DefaultSupport implements JdbcArrayColumns {

		INSTANCE;

		@Override
		public boolean isSupported() {
			return true;
		}

		@Override
		public String getArrayTypeName(SQLType jdbcType) {
			return jdbcType.getName();
		}

	}

}
