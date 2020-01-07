/*
 * Copyright 2018-2020 the original author or authors.
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

import org.springframework.test.annotation.ProfileValueSource;

/**
 * This {@link ProfileValueSource} offers a single set of keys {@code current.database.is.not.<database>} where
 * {@code <database> } is a database as used in active profiles to enable integration tests to run with a certain
 * database. The value returned for these keys is {@code "true"} or {@code "false"} depending on if the database is
 * actually the one currently used by integration tests.
 *
 * @author Jens Schauder
 */
public class DatabaseProfileValueSource implements ProfileValueSource {

	private final String currentDatabase;

	DatabaseProfileValueSource() {

		currentDatabase = System.getProperty("spring.profiles.active", "hsqldb");
	}

	@Override
	public String get(String key) {

		if (!key.startsWith("current.database.is.not.")) {
			return null;
		}

		return Boolean.toString(!key.endsWith(currentDatabase)).toLowerCase();
	}
}
