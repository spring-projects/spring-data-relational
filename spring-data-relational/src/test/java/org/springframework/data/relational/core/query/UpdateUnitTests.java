/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.relational.core.query;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Update}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class UpdateUnitTests {

	@Test // DATAJDBC-513
	void shouldRenderUpdateToString() {
		assertThat(Update.update("foo", "baz").set("bar", 42)).hasToString("SET foo = 'baz', bar = 42");
	}

	@Test // GH-2226
	void shouldRenderUpdateWithTypedPropertyPathToString() {
		assertThat(Update.update(Person::getFirstName, "baz").set("bar", 42)).hasToString("SET firstName = 'baz', bar = 42");
	}

	static class Person {
		private String firstName;
		private String lastName;

		String getFirstName() {
			return firstName;
		}

		public String getLastName() {
			return lastName;
		}
	}
}
