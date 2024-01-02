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

package org.springframework.data.relational.core.sqlgeneration;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.sqlgeneration.SqlAssert.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for SqlAssert.
 * @author Jens Schauder
 */
class SqlAssertUnitTests {

	@Test // GH-1446
	void givesProperNullPointerExceptionWhenSqlIsNull() {
		assertThatThrownBy(() -> SqlAssert.assertThatParsed(null)).isInstanceOf(NullPointerException.class);
	}

	@Nested
	class AssertWhereClause {
		@Test // GH-1446
		void assertWhereClause() {
			SqlAssert.assertThatParsed("select x from t where z > y").extractWhereClause().isEqualTo("z > y");
		}

		@Test // GH-1446
		void assertNoWhereClause() {
			SqlAssert.assertThatParsed("select x from t").extractWhereClause().isEmpty();
		}

	}

	@Nested
	class AssertOrderByClause {
		@Test // GH-1446
		void assertOrderByClause() {
			SqlAssert.assertThatParsed("select x from t order by x, y").extractOrderBy().isEqualTo("x, y");
		}

		@Test // GH-1446
		void assertNoOrderByClause() {
			SqlAssert.assertThatParsed("select x from t").extractOrderBy().isEmpty();
		}

	}

	@Nested
	class AssertColumns {
		@Test // GH-1446
		void matchingSimpleColumns() {
			SqlAssert.assertThatParsed("select x, y, z from t").hasExactlyColumns("x", "y", "z");
		}

		@Test // GH-1446
		void extraSimpleColumn() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x, y, z, a from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns("x", "y", "z")) //
					.hasMessageContaining("x, y, z") //
					.hasMessageContaining("x, y, z, a") //
					.hasMessageContaining("a");
		}

		@Test // GH-1446
		void missingSimpleColumn() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x, y, z from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns("x", "y", "z", "a")) //
					.hasMessageContaining("x, y, z") //
					.hasMessageContaining("x, y, z, a") //
					.hasMessageContaining("a");
		}

		@Test // GH-1446
		void wrongSimpleColumn() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x, y, z from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns("x", "a", "z")) //
					.hasMessageContaining("x, y, z") //
					.hasMessageContaining("x, a, z") //
					.hasMessageContaining("a") //
					.hasMessageContaining("y");
		}

		@Test // GH-1446
		void matchesFullyQualifiedColumn() {
			
			SqlAssert.assertThatParsed("select t.x from t") //
					.hasExactlyColumns("x");
		}

		@Test // GH-1446
		void matchesFunction() {                                             //
			
			SqlAssert.assertThatParsed("select someFunc(x) from t")
					.hasExactlyColumns(func("someFunc", col("x")));
		}

		@Test // GH-1446
		void matchesFunctionCaseInsensitive() {
			
			SqlAssert.assertThatParsed("select COUNT(x) from t") //
					.hasExactlyColumns(func("count", col("x")));
		}

		@Test // GH-1446
		void matchFunctionFailsOnDifferentName() {
			SqlAssert sqlAssert = assertThatParsed("select countx(x) from t");
			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(func("count", col("x")))) //
					.hasMessageContaining("countx(x)") //
					.hasMessageContaining("count(x)");
		}

		@Test // GH-1446
		void matchFunctionFailsOnDifferentParameter() {

			SqlAssert sqlAssert = assertThatParsed("select count(y) from t");
			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(func("count", col("x")))) //
					.hasMessageContaining("count(y)") //
					.hasMessageContaining("count(x)");
		}

		@Test // GH-1446
		void matchFunctionFailsOnWrongParameterCount() {

			SqlAssert sqlAssert = assertThatParsed("select count(x, y) from t");
			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(func("count", col("x")))) //
					.hasMessageContaining("count(x, y)") //
					.hasMessageContaining("count(x)");
		}
	}

	@Nested
	class AssertRowNumber {
		@Test // GH-1446
		void testMatchingRowNumber() {

			SqlAssert sqlAssert = assertThatParsed("select row_number() over (partition by x) from t");

			sqlAssert.hasExactlyColumns(rn(col("x")));
		}

		@Test // GH-1446
		void testMatchingRowNumberUpperCase() {

			SqlAssert sqlAssert = assertThatParsed("select ROW_NUMBER() over (partition by x) from t");

			sqlAssert.hasExactlyColumns(rn(col("x")));
		}

		@Test // GH-1446
		void testFailureNoRowNumber() {

			SqlAssert sqlAssert = assertThatParsed("select row_number as x from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(rn(col("x")))) //
					.hasMessageContaining("row_number AS x") //
					.hasMessageContaining("row_number() OVER (PARTITION BY x)");
			;
		}

		@Test // GH-1446
		void testFailureWrongPartitionBy() {

			SqlAssert sqlAssert = assertThatParsed("select row_number() over (partition by y) from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(rn(col("x")))) //
					.hasMessageContaining("row_number() OVER (PARTITION BY y )") //
					.hasMessageContaining("row_number() OVER (PARTITION BY x)");
		}
	}

	@Nested
	class AssertAliases {
		@Test // GH-1446
		void simpleColumnMatchesWithAlias() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x as a from t");

			sqlAssert.hasExactlyColumns("x");
		}

		@Test // GH-1446
		void matchWithAlias() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x as a from t");

			sqlAssert.hasExactlyColumns(col("x").as("a"));
		}

		@Test // GH-1446
		void matchWithWrongAlias() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select x as b from t");

			assertThatThrownBy(() -> sqlAssert.hasExactlyColumns(col("x").as("a"))) //
					.hasMessageContaining("x as a") //
					.hasMessageContaining("x AS b");
		}

		@Test // GH-1446
		void matchesIdenticalColumnsWithDifferentAliases() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select 1 as x, 1 as y from t");

			sqlAssert.hasExactlyColumns(lit(1).as("x"), lit(1).as("y"));
		}
	}

	@Nested
	class AssertSubSelects {
		@Test // GH-1446
		void subselectGetsFound() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select a from (select x as a from t) s");

			sqlAssert //
					.hasInlineViewSelectingFrom("t") //
					.hasExactlyColumns(col("x").as("a"));
		}

		@Test // GH-1446
		void subselectWithWrongTableDoesNotGetFound() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select a from (select x as a from u) s");

			assertThatThrownBy(() -> sqlAssert //
					.hasInlineViewSelectingFrom("t"))
							.hasMessageContaining("is expected to contain a subselect selecting from t but doesn't");
		}
	}

	@Nested
	class AssertJoins {
		@Test // GH-1446
		void hasJoin() {
			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select c from t join s on x = y");

			sqlAssert.hasJoin();
		}

		@Test // GH-1446
		void hasJoinFailure() {
			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select c from t where x = y");

			assertThatThrownBy(() -> sqlAssert //
					.hasJoin()).hasMessageContaining("to contain a join but it doesn't");
		}

		@Test // GH-1446
		void on() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select c from t join s on x = y");

			sqlAssert.hasJoin().on("x", "y");
		}

		@Test // GH-1446
		void onFailureFirst() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select c from t join s on z = y");

			assertThatThrownBy(() -> sqlAssert.hasJoin().on("x", "y"))
					.hasMessageContaining("z = y does not match expected x = y");
		}

		@Test // GH-1446
		void onFailureSecond() {

			SqlAssert sqlAssert = SqlAssert.assertThatParsed("select c from t join s on x = z");

			assertThatThrownBy(() -> sqlAssert.hasJoin().on("x", "y"))
					.hasMessageContaining("x = z does not match expected x = y");
		}

	}
}
