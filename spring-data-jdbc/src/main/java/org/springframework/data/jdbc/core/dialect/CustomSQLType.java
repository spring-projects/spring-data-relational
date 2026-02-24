/*
 * Copyright 2026-present the original author or authors.
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

import java.sql.SQLType;

/**
 * Record implementation of {@link SQLType} to be used for custom types in {@link JdbcDialect}.
 *
 * @author Mark Paluch
 * @since 4.0.4
 */
record CustomSQLType(String name, String vendor, int oid) implements SQLType {

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getVendor() {
		return vendor;
	}

	@Override
	public Integer getVendorTypeNumber() {
		return oid;
	}

}
