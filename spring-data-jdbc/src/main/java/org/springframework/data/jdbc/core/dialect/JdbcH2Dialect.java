/*
 * Copyright 2025-present the original author or authors.
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

import org.springframework.data.relational.core.dialect.H2Dialect;

/**
 * JDBC-specific H2 Dialect.
 *
 * @author Mikhail Polivakha
 * @since 3.5
 */
public class JdbcH2Dialect extends H2Dialect implements JdbcDialect {

	public static final JdbcH2Dialect INSTANCE = new JdbcH2Dialect();

	private static final JdbcArrayColumns ARRAY_COLUMNS = new JdbcArrayColumnsAdapter(H2ArrayColumns.INSTANCE);

	@Override
	public JdbcArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

}
