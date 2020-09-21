/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SettableValue}.
 *
 * @author Mark Paluch
 */
class SettableValueUnitTests {

	@Test // gh-59
	void shouldCreateSettableValue() {

		SettableValue value = SettableValue.from("foo");

		assertThat(value.isEmpty()).isFalse();
		assertThat(value.hasValue()).isTrue();
		assertThat(value).isEqualTo(SettableValue.from("foo"));
	}

	@Test // gh-59
	void shouldCreateEmpty() {

		SettableValue value = SettableValue.empty(Object.class);

		assertThat(value.isEmpty()).isTrue();
		assertThat(value.hasValue()).isFalse();
		assertThat(value).isEqualTo(SettableValue.empty(Object.class));
		assertThat(value).isNotEqualTo(SettableValue.empty(String.class));
	}

	@Test // gh-59
	void shouldCreatePotentiallyEmpty() {

		assertThat(SettableValue.fromOrEmpty("foo", Object.class).isEmpty()).isFalse();
		assertThat(SettableValue.fromOrEmpty(null, Object.class).isEmpty()).isTrue();
	}
}
