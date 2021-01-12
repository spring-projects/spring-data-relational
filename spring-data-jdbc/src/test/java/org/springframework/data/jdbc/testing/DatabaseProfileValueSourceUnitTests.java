/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.jdbc.testing.DatabaseProfileValueSource.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DatabaseProfileValueSource}.
 *
 * @author Jens Schauder
 */
public class DatabaseProfileValueSourceUnitTests {

	String oldSystemPropertyValue;

	@Before
	public void before() {
		oldSystemPropertyValue = System.getProperty(SPRING_PROFILES_ACTIVE);
	}

	@After
	public void after() {

		if (oldSystemPropertyValue == null) {
			System.clearProperty(SPRING_PROFILES_ACTIVE);
		} else {
			System.setProperty(SPRING_PROFILES_ACTIVE, oldSystemPropertyValue);
		}
	}

	@Test // DATAJDBC-461
	public void returnNullForUnrelatedProperty() {

		DatabaseProfileValueSource source = new DatabaseProfileValueSource();
		assertThat(source.get("blah")).isNull();
	}

	@Test // DATAJDBC-461
	public void worksWithSystemProperty() {

		System.setProperty(SPRING_PROFILES_ACTIVE, "testProfile");

		DatabaseProfileValueSource source = new DatabaseProfileValueSource();

		assertThat(source.get(CURRENT_DATABASE_IS_NOT + "other")).isEqualTo("true");
		assertThat(source.get(CURRENT_DATABASE_IS_NOT + "testProfile")).isEqualTo("false");
	}

}
