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

import org.springframework.util.Assert;

/**
 * Value object to represent the underlying test class.
 *
 * @author Mark Paluch
 */
public final class TestClass {

	private final Class<?> testClass;

	private TestClass(Class<?> testClass) {
		this.testClass = testClass;
	}

	/**
	 * Create a new {@link TestClass} given {@code testClass}.
	 *
	 * @param testClass must not be {@literal null}.
	 * @return the new {@link TestClass}.
	 */
	public static TestClass of(Class<?> testClass) {

		Assert.notNull(testClass, "TestClass must not be null");

		return new TestClass(testClass);
	}

	public Class<?> getTestClass() {
		return testClass;
	}
}
