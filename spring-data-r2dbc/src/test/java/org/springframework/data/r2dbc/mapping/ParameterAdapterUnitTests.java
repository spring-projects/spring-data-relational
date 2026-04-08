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
package org.springframework.data.r2dbc.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.Parameter;

/**
 * Unit tests for {@link ParameterAdapter}.
 *
 * @author Christoph Strobl
 */
class ParameterAdapterUnitTests {

	@Test // GH-493
	void adaptersWrappingSameDelegateAreEqualAndShareHashCode() {

		Parameter delegate = Parameter.from("x");
		ParameterAdapter first = new ParameterAdapter(delegate);
		ParameterAdapter second = new ParameterAdapter(delegate);

		assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
	}

	@Test // GH-493
	void adaptersWrappingEqualDelegatesAreEqual() {

		ParameterAdapter first = new ParameterAdapter(Parameter.from("x"));
		ParameterAdapter second = new ParameterAdapter(Parameter.from("x"));

		assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
	}

	@Test // GH-493
	void adaptersWrappingNullDelegatesAreEqual() {

		ParameterAdapter first = new ParameterAdapter(null);
		ParameterAdapter second = new ParameterAdapter(null);

		assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
	}

	@Test // GH-493
	void adaptersWrappingDifferentDelegatesAreNotEqual() {

		ParameterAdapter first = new ParameterAdapter(Parameter.from("a"));
		ParameterAdapter second = new ParameterAdapter(Parameter.from("b"));

		assertThat(first).isNotEqualTo(second);
	}

	@Test // GH-493
	void adapterEqualsWrappedSpringParameterWhenDelegateMatches() {

		Parameter delegate = Parameter.from("x");
		ParameterAdapter adapter = new ParameterAdapter(delegate);

		assertThat(adapter).isEqualTo(delegate);
	}
}
