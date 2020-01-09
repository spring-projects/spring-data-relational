/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.assertj.core.api.SoftAssertions;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Tests for {@link SqlIdentifierParameterSource}.
 *
 * @author Jens Schauder
 */
public class SqlIdentifierParameterSourceUnitTests {

	private IdentifierProcessing identifierProcessing = IdentifierProcessing.ANSI;

	@Test // DATAJDBC-386
	public void empty() {

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(identifierProcessing);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(parameters.getParameterNames()).isEmpty();
			softly.assertThat(parameters.getValue("blah")).isNull();
			softly.assertThat(parameters.hasValue("blah")).isFalse();
			softly.assertThat(parameters.getSqlType("blah")).isEqualTo(Integer.MIN_VALUE);
		});
	}

	@Test // DATAJDBC-386
	public void addSingleValue() {

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(identifierProcessing);

		parameters.addValue(SqlIdentifier.unquoted("key"), 23);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(parameters.getParameterNames()).isEqualTo(new String[] { "key" });
			softly.assertThat(parameters.getValue("key")).isEqualTo(23);
			softly.assertThat(parameters.hasValue("key")).isTrue();

			softly.assertThat(parameters.getValue("blah")).isNull();
			softly.assertThat(parameters.hasValue("blah")).isFalse();
			softly.assertThat(parameters.getSqlType("blah")).isEqualTo(Integer.MIN_VALUE);
		});
	}

	@Test // DATAJDBC-386
	public void addSingleValueWithType() {

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(identifierProcessing);

		parameters.addValue(SqlIdentifier.unquoted("key"), 23, 42);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(parameters.getParameterNames()).isEqualTo(new String[] { "key" });
			softly.assertThat(parameters.getValue("key")).isEqualTo(23);
			softly.assertThat(parameters.hasValue("key")).isTrue();
			softly.assertThat(parameters.getSqlType("key")).isEqualTo(42);

			softly.assertThat(parameters.getValue("blah")).isNull();
			softly.assertThat(parameters.hasValue("blah")).isFalse();
			softly.assertThat(parameters.getSqlType("blah")).isEqualTo(Integer.MIN_VALUE);
		});
	}

	@Test // DATAJDBC-386
	public void addOtherDatabaseObjectIdentifierParameterSource() {

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(identifierProcessing);
		parameters.addValue(SqlIdentifier.unquoted("key1"), 111, 11);
		parameters.addValue(SqlIdentifier.unquoted("key2"), 111);

		SqlIdentifierParameterSource parameters2 = new SqlIdentifierParameterSource(identifierProcessing);
		parameters2.addValue(SqlIdentifier.unquoted("key2"), 222, 22);
		parameters2.addValue(SqlIdentifier.unquoted("key3"), 222);

		parameters.addAll(parameters2);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(parameters.getParameterNames()).isEqualTo(new String[] { "key1", "key2", "key3" });
			softly.assertThat(parameters.getValue("key1")).isEqualTo(111);
			softly.assertThat(parameters.hasValue("key1")).isTrue();
			softly.assertThat(parameters.getSqlType("key1")).isEqualTo(11);

			softly.assertThat(parameters.getValue("key2")).isEqualTo(222);
			softly.assertThat(parameters.hasValue("key2")).isTrue();
			softly.assertThat(parameters.getSqlType("key2")).isEqualTo(22);

			softly.assertThat(parameters.getValue("key3")).isEqualTo(222);
			softly.assertThat(parameters.hasValue("key3")).isTrue();
			softly.assertThat(parameters.getSqlType("key3")).isEqualTo(Integer.MIN_VALUE);

			softly.assertThat(parameters.getValue("blah")).isNull();
			softly.assertThat(parameters.hasValue("blah")).isFalse();
			softly.assertThat(parameters.getSqlType("blah")).isEqualTo(Integer.MIN_VALUE);
		});
	}
}
