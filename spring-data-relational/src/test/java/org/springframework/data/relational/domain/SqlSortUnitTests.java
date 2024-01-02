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

package org.springframework.data.relational.domain;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;

/**
 * Unit tests for {@link SqlSort} and
 * {@link SqlSort.SqlOrder}.
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
}
