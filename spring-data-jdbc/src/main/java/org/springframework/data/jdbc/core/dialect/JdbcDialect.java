/*
 * Copyright 2021-2025 the original author or authors.
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

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.jdbc.core.dialect.SqlTypeResolver;
import org.springframework.data.jdbc.core.dialect.DefaultSqlTypeResolver;

/**
 * {@link org.springframework.data.relational.core.dialect.ArrayColumns} that offer JDBC specific functionality.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 2.3
 */
public interface JdbcDialect extends Dialect {

	/**
	 * Returns the JDBC specific array support object that describes how array-typed columns are supported by this
	 * dialect.
	 *
	 * @return the JDBC specific array support object that describes how array-typed columns are supported by this
	 *         dialect.
	 */
	default JdbcArrayColumns getArraySupport() {
		return JdbcArrayColumns.Unsupported.INSTANCE;
	}

	/**
	 * Returns a {@link SqlTypeResolver} of this dialect.
	 *
	 * @since 4.0
	 */
	default SqlTypeResolver getSqlTypeResolver() {
		return DefaultSqlTypeResolver.INSTANCE;
	}
}
