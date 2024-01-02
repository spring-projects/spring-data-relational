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

package org.springframework.data.jdbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * Tests for the {@link EscapingParameterSource}.
 *
 * @author Jens Schauder
 */
class EscapingParameterSourceUnitTests {

	MapSqlParameterSource delegate = new MapSqlParameterSource();
	Escaper escaper = Escaper.of('x');
	EscapingParameterSource escapingParameterSource = new EscapingParameterSource(delegate, escaper);

	@Nested
	class EmptyParameterSource {

		@Test
		void getParameterNames() {
			assertThat(escapingParameterSource.getParameterNames()).isEmpty();
		}

		@Test
		void hasValue() {
			assertThat(escapingParameterSource.hasValue("one")).isFalse();
		}

		@Test
		void getNonExistingValue() {
			assertThatIllegalArgumentException().isThrownBy(() -> escapingParameterSource.getValue("two"));
		}

	}

	@Nested
	class NonEmptyParameterSource {

		@BeforeEach
		void before() {
			delegate.addValue("one", 1, Types.INTEGER);
			delegate.registerTypeName("one", "integer");
			delegate.addValue("needsEscaping", (ValueFunction<String>) escaper -> escaper.escape("a%a") + "%", Types.VARCHAR);
			delegate.registerTypeName("needsEscaping", "varchar");
		}

		@Test
		void getParameterNames() {
			assertThat(escapingParameterSource.getParameterNames()).containsExactlyInAnyOrder("one", "needsEscaping");
		}

		@Test
		void hasValue() {
			assertThat(escapingParameterSource.hasValue("one")).isTrue();
			assertThat(escapingParameterSource.hasValue("two")).isFalse();
		}

		@Test
		void getNonExistingValue() {
			assertThatIllegalArgumentException().isThrownBy(() -> escapingParameterSource.getValue("two"));
		}

		@Test
		void getValue() {
			assertThat(escapingParameterSource.getValue("one")).isEqualTo(1);
		}

		@Test
		void getEscapedValue() {
			assertThat(escapingParameterSource.getValue("needsEscaping")).isEqualTo("ax%a%");
		}

		@Test
		void getSqlType() {

			assertThat(escapingParameterSource.getSqlType("one")).isEqualTo(Types.INTEGER);
			assertThat(escapingParameterSource.getSqlType("needsEscaping")).isEqualTo(Types.VARCHAR);
		}

		@Test
		void getTypeName() {

			assertThat(escapingParameterSource.getTypeName("one")).isEqualTo("integer");
			assertThat(escapingParameterSource.getTypeName("needsEscaping")).isEqualTo("varchar");
		}
	}
}
