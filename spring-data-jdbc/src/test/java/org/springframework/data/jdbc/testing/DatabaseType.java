/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.testing;

import java.util.Locale;

/**
 * Supported database types. Types are defined to express against which database a particular test is expected to run.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public enum DatabaseType {

	DB2, HSQL, H2, MARIADB, MYSQL, ORACLE, POSTGRES, SQL_SERVER("mssql");

	private final String profile;

	DatabaseType() {
		this.profile = name().toLowerCase(Locale.ROOT);
	}

	DatabaseType(String profile) {
		this.profile = profile;
	}

	/**
	 * @return the profile string as used in Spring profiles.
	 */
	public String getProfile() {
		return profile;
	}
}
