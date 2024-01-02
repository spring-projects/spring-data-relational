/*
 * Copyright 2017-2024 the original author or authors.
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

import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;

/**
 * Utility methods for testing.
 *
 * @author Oliver Gierke
 */
public interface TestUtils {

	/**
	 * Returns the name of the SQL script to be loaded for the given test class and database type.
	 *
	 * @param testClass must not be {@literal null}.
	 * @param databaseType must not be {@literal null} or empty.
	 * @return
	 */
	public static String createScriptName(Class<?> testClass, String databaseType) {

		Assert.notNull(testClass, "Test class must not be null");
		Assert.hasText(databaseType, "Database type must not be null or empty");

		String path = String.format("%s/%s-%s.sql", testClass.getPackage().getName(), testClass.getSimpleName(),
				databaseType.toLowerCase());

		ClassPathResource resource = new ClassPathResource(path);
		if (!resource.exists()) {
			throw new IllegalStateException("Test resource " + path + " not found");
		}

		return path;
	}
}
