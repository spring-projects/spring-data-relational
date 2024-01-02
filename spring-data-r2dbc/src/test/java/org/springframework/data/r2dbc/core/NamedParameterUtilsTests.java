/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * Unit tests for {@link NamedParameterUtils}.
 *
 * @author Mark Paluch
 */
class NamedParameterUtilsTests {

	@Test // GH-1306
	void inCollectionSameParameterNameShouldBindAllAnonymousParameters() {

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select :names AND :names");
		PreparedOperation<String> operation = NamedParameterUtils.substituteNamedParameters(parsedSql,
				BindMarkersFactory.anonymous("?"),
				new MapBindParameterSource(Collections.singletonMap("names", Parameter.from(Arrays.asList("1", "2", "3")))));

		List<String> bindings = new ArrayList<>();

		operation.bindTo(new BindingCaptor(bindings));

		assertThat(operation.get()).isEqualTo("select ?, ?, ? AND ?, ?, ?");
		assertThat(bindings).contains("0: 1", "1: 2", "2: 3", "3: 1", "4: 2", "5: 3");
	}

	@Test // GH-1306
	void complexInCollectionSameParameterNameShouldBindAllAnonymousParameters() {

		Map<String, Parameter> parameterMap = new HashMap<>();
		parameterMap.put("names", Parameter.from(Arrays.asList("1", "2", "3")));
		parameterMap.put("hello", Parameter.from("world"));

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select :names AND :hello OR :names");
		PreparedOperation<String> operation = NamedParameterUtils.substituteNamedParameters(parsedSql,
				BindMarkersFactory.anonymous("?"), new MapBindParameterSource(parameterMap));

		List<String> bindings = new ArrayList<>();

		operation.bindTo(new BindingCaptor(bindings));

		assertThat(operation.get()).isEqualTo("select ?, ?, ? AND ? OR ?, ?, ?");
		assertThat(bindings).contains("0: 1", "1: 2", "2: 3", "3: world", "4: 1", "5: 2", "6: 3");
	}

	@Test // GH-1306
	void inCollectionSameParameterNameShouldBindAllNamedParameters() {

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement("select :names AND :names");
		PreparedOperation<String> operation = NamedParameterUtils.substituteNamedParameters(parsedSql,
				BindMarkersFactory.indexed("$", 1),
				new MapBindParameterSource(Collections.singletonMap("names", Parameter.from(Arrays.asList("1", "2", "3")))));

		List<String> bindings = new ArrayList<>();

		operation.bindTo(new BindingCaptor(bindings));

		assertThat(operation.get()).isEqualTo("select $1, $2, $3 AND $1, $2, $3");
		assertThat(bindings).containsOnly("0: 1", "1: 2", "2: 3");
	}

	static class BindingCaptor implements BindTarget {

		private final List<String> bindings;

		BindingCaptor(List<String> bindings) {
			this.bindings = bindings;
		}

		@Override
		public void bind(String identifier, Object value) {
			bindings.add(identifier + ": " + value);
		}

		@Override
		public void bind(int index, Object value) {
			bindings.add(index + ": " + value);
		}

		@Override
		public void bindNull(String identifier, Class<?> type) {

		}

		@Override
		public void bindNull(int index, Class<?> type) {

		}

	}
}
