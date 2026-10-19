/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.data.relational.domain;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Sort;

/**
 * Unit tests for {@link SqlSort} and {@link SqlSort.SqlOrder}.
 *
 * @author Jens Schauder
 */
class SqlSortUnitTests {

	@Test
	void sortOfDirectionAndProperties() {

		SqlSort sort = SqlSort.of(Sort.Direction.DESC, "firstName", "lastName");

		assertThat(sort).containsExactly( //
				SqlSort.SqlOrder.desc("firstName"), //
				SqlSort.SqlOrder.desc("lastName") //
		);
	}

	@Test
	void unsafeSortOfProperties() {

		SqlSort sort = SqlSort.unsafe("firstName", "lastName");

		assertThat(sort).containsExactly( //
				SqlSort.SqlOrder.by("firstName"), //
				SqlSort.SqlOrder.by("lastName") //
		);
	}

	@Test
	void mixingDirections() {

		SqlSort sort = SqlSort.of("firstName").and(Sort.Direction.DESC, "lastName", "address");

		assertThat(sort).containsExactly( //
				SqlSort.SqlOrder.asc("firstName"), //
				SqlSort.SqlOrder.desc("lastName"), //
				SqlSort.SqlOrder.desc("address") //
		);
	}

	@Test
	void mixingDirectionsAndSafety() {

		SqlSort sort = SqlSort.of("firstName").andUnsafe(Sort.Direction.DESC, "lastName", "address");

		assertThat(sort).containsExactly( //
				SqlSort.SqlOrder.by("firstName"), //
				SqlSort.SqlOrder.desc("lastName").withUnsafe(), //
				SqlSort.SqlOrder.desc("address").withUnsafe() //
		);
	}

	@Test
	void orderDoesNotDependOnOrderOfMethodCalls() {

		assertThat(
				SqlSort.SqlOrder.desc("property").ignoreCase().withUnsafe().with(Sort.NullHandling.NULLS_LAST))
						.isEqualTo(SqlSort.SqlOrder.by("property").with(Sort.NullHandling.NULLS_LAST).withUnsafe()
								.ignoreCase().with(Sort.Direction.DESC));
	}

	@ParameterizedTest // GH-2378
	@ValueSource(strings = { "firstName", "_firstName", "person.firstName", "person._first_name_2", "x._x", "员工编号",
			"员工.姓名", "Αναγνωριστικό_εργαζομένου", "Співробітники", "Çalışanlar", "José", "कर्मचारी", "வேலை", "วันที่" })
	void validateAcceptsIdentifiers(String property) {
		assertThatNoException().isThrownBy(() -> SqlSort.validate(Sort.Order.by(property)));
	}

	@ParameterizedTest // GH-2378
	@ValueSource(strings = { "first name", "first;name", "first'name", "first\"name", "first-name", "--",
			"lower(name)", "(x)", "count(*)", "1=1", "name/**/", "name#", "name«bar", "name“bar", "½", "Ⅳ",
			"①" })
	void validateRejectsEverythingElse(String property) {

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class) //
				.isThrownBy(() -> SqlSort.validate(Sort.Order.by(property))) //
				.withMessageContaining("must only contain property references");
	}

	@ParameterizedTest // GH-2378
	@ValueSource(strings = { "name\n", "name\r", "name\r\n", "name\u0085", "name\u2028", "name\u2029" })
	void validateRejectsTrailingLineTerminators(String property) {

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> SqlSort.validate(Sort.Order.by(property)));
	}

	@ParameterizedTest // GH-2378
	@ValueSource(strings = { "lower(name)", "sum(foobar)", "name; DROP TABLE person" })
	void validateAcceptsAnythingMarkedUnsafe(String property) {
		assertThatNoException().isThrownBy(() -> SqlSort.validate(SqlSort.SqlOrder.by(property).withUnsafe()));
	}
}
