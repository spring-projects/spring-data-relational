/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.query.Criteria.*;

import java.util.Arrays;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 */
class CriteriaUnitTests {

	@Test // gh-289
	void fromCriteria() {

		Criteria nested1 = where("foo").isNotNull();
		Criteria nested2 = where("foo").isNull();
		Criteria criteria = Criteria.from(nested1, nested2);

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).containsExactly(nested1, nested2);
		assertThat(criteria.getPrevious()).isEqualTo(Criteria.empty());
	}

	@Test // gh-289
	void fromCriteriaOptimized() {

		Criteria nested = where("foo").is("bar").and("baz").isNotNull();
		Criteria criteria = Criteria.from(nested);

		assertThat(criteria).isSameAs(nested);
	}

	@Test // gh-289
	void isEmpty() {

		SoftAssertions.assertSoftly(softly -> {

			Criteria empty = empty();
			Criteria notEmpty = where("foo").is("bar");

			assertThat(empty.isEmpty()).isTrue();
			assertThat(notEmpty.isEmpty()).isFalse();

			assertThat(Criteria.from(notEmpty).isEmpty()).isFalse();
			assertThat(Criteria.from(notEmpty, notEmpty).isEmpty()).isFalse();

			assertThat(Criteria.from(empty).isEmpty()).isTrue();
			assertThat(Criteria.from(empty, empty).isEmpty()).isTrue();

			assertThat(Criteria.from(empty, notEmpty).isEmpty()).isFalse();
			assertThat(Criteria.from(notEmpty, empty).isEmpty()).isFalse();
		});
	}

	@Test // gh-64
	void andChainedCriteria() {

		Criteria criteria = where("foo").is("bar").and("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
		assertThat(criteria.getValue()).isNull();
		assertThat(criteria.getPrevious()).isNotNull();
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-289
	void andGroupedCriteria() {

		Criteria criteria = where("foo").is("bar").and(where("foo").is("baz"));

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).hasSize(1);
		assertThat(criteria.getGroup().get(0).getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	void orChainedCriteria() {

		Criteria criteria = where("foo").is("bar").or("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getPrevious()).isNull();
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-289
	void orGroupedCriteria() {

		Criteria criteria = where("foo").is("bar").or(where("foo").is("baz"));

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).hasSize(1);
		assertThat(criteria.getGroup().get(0).getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	void shouldBuildEqualsCriteria() {

		Criteria criteria = where("foo").is("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test
	void shouldBuildEqualsIgnoreCaseCriteria() {
		Criteria criteria = where("foo").is("bar").ignoreCase(true);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
		assertThat(criteria.isIgnoreCase()).isTrue();
	}

	@Test // gh-64
	void shouldBuildNotEqualsCriteria() {

		Criteria criteria = where("foo").not("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NEQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	void shouldBuildInCriteria() {

		Criteria criteria = where("foo").in("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	void shouldBuildNotInCriteria() {

		Criteria criteria = where("foo").notIn("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NOT_IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	void shouldBuildGtCriteria() {

		Criteria criteria = where("foo").greaterThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	void shouldBuildGteCriteria() {

		Criteria criteria = where("foo").greaterThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	void shouldBuildLtCriteria() {

		Criteria criteria = where("foo").lessThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	void shouldBuildLteCriteria() {

		Criteria criteria = where("foo").lessThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	void shouldBuildLikeCriteria() {

		Criteria criteria = where("foo").like("hello%");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test
	void shouldBuildNotLikeCriteria() {
		Criteria criteria = where("foo").notLike("hello%");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NOT_LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test // gh-64
	void shouldBuildIsNullCriteria() {

		Criteria criteria = where("foo").isNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NULL);
	}

	@Test // gh-64
	void shouldBuildIsNotNullCriteria() {

		Criteria criteria = where("foo").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
	}

	@Test // gh-282
	void shouldBuildIsTrueCriteria() {

		Criteria criteria = where("foo").isTrue();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_TRUE);
	}

	@Test // gh-282
	void shouldBuildIsFalseCriteria() {

		Criteria criteria = where("foo").isFalse();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_FALSE);
	}
}
