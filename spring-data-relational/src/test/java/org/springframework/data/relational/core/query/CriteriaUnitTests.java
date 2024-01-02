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
package org.springframework.data.relational.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.query.Criteria.*;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Roman Chigvintsev
 */
class CriteriaUnitTests {

	@Test // DATAJDBC-513
	void fromCriteria() {

		Criteria nested1 = where("foo").isNotNull();
		Criteria nested2 = where("foo").isNull();
		CriteriaDefinition criteria = Criteria.from(nested1, nested2);

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).containsExactly(nested1, nested2);
		assertThat(criteria.getPrevious()).isEqualTo(Criteria.empty());
		assertThat(criteria).hasToString("(foo IS NOT NULL AND foo IS NULL)");
	}

	@Test // DATAJDBC-513
	void fromCriteriaOptimized() {

		Criteria nested = where("foo").is("bar").and("baz").isNotNull();
		CriteriaDefinition criteria = Criteria.from(nested);

		assertThat(criteria).isSameAs(nested).hasToString("foo = 'bar' AND baz IS NOT NULL");
	}

	@Test // DATAJDBC-513
	void isEmpty() {

		assertSoftly(softly -> {

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

	@Test // DATAJDBC-513
	void andChainedCriteria() {

		Criteria criteria = where("foo").is("bar").and("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IS_NOT_NULL);
		assertThat(criteria.getValue()).isNull();
		assertThat(criteria.getPrevious()).isNotNull();
		assertThat(criteria.getCombinator()).isEqualTo(Criteria.Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // DATAJDBC-513
	void andGroupedCriteria() {

		Criteria grouped = where("foo").is("bar").and(where("foo").is("baz").or("bar").isNotNull());
		Criteria criteria = grouped;

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).hasSize(1);
		assertThat(criteria.getGroup().get(0).getColumn()).isEqualTo(SqlIdentifier.unquoted("bar"));
		assertThat(criteria.getCombinator()).isEqualTo(Criteria.Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");

		assertThat(grouped).hasToString("foo = 'bar' AND (foo = 'baz' OR bar IS NOT NULL)");
	}

	@Test // DATAJDBC-513
	void orChainedCriteria() {

		Criteria criteria = where("foo").is("bar").or("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getCombinator()).isEqualTo(Criteria.Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getPrevious()).isNull();
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // DATAJDBC-513
	void orGroupedCriteria() {

		Criteria criteria = where("foo").is("bar").or(where("foo").is("baz"));

		assertThat(criteria.isGroup()).isTrue();
		assertThat(criteria.getGroup()).hasSize(1);
		assertThat(criteria.getGroup().get(0).getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getCombinator()).isEqualTo(Criteria.Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria).isNotNull();
		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // DATAJDBC-513
	void shouldBuildEqualsCriteria() {

		Criteria criteria = where("foo").is("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test
	void shouldBuildEqualsIgnoreCaseCriteria() {
		Criteria criteria = where("foo").is("bar").ignoreCase(true);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
		assertThat(criteria.isIgnoreCase()).isTrue();
	}

	@Test // DATAJDBC-513
	void shouldBuildNotEqualsCriteria() {

		Criteria criteria = where("foo").not("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.NEQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // DATAJDBC-513
	void shouldBuildInCriteria() {

		Criteria criteria = where("foo").in("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
		assertThat(criteria).hasToString("foo IN ('bar', 'baz')");
	}

	@Test // DATAJDBC-513
	void shouldBuildNotInCriteria() {

		Criteria criteria = where("foo").notIn("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.NOT_IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // DATAJDBC-513
	void shouldBuildGtCriteria() {

		Criteria criteria = where("foo").greaterThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.GT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // DATAJDBC-513
	void shouldBuildGteCriteria() {

		Criteria criteria = where("foo").greaterThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.GTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // DATAJDBC-513
	void shouldBuildLtCriteria() {

		Criteria criteria = where("foo").lessThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.LT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // DATAJDBC-513
	void shouldBuildLteCriteria() {

		Criteria criteria = where("foo").lessThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.LTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // DATAJDBC-513
	void shouldBuildLikeCriteria() {

		Criteria criteria = where("foo").like("hello%");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test
	void shouldBuildNotLikeCriteria() {
		Criteria criteria = where("foo").notLike("hello%");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.NOT_LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test // DATAJDBC-513
	void shouldBuildIsNullCriteria() {

		Criteria criteria = where("foo").isNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IS_NULL);
	}

	@Test // DATAJDBC-513
	void shouldBuildIsNotNullCriteria() {

		Criteria criteria = where("foo").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IS_NOT_NULL);
	}

	@Test // DATAJDBC-513
	void shouldBuildIsTrueCriteria() {

		Criteria criteria = where("foo").isTrue();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IS_TRUE);
		assertThat(criteria.getValue()).isEqualTo(true);
	}

	@Test // DATAJDBC-513
	void shouldBuildIsFalseCriteria() {

		Criteria criteria = where("foo").isFalse();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(CriteriaDefinition.Comparator.IS_FALSE);
		assertThat(criteria.getValue()).isEqualTo(false);
	}
}
