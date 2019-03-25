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
package org.springframework.data.r2dbc.function.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.r2dbc.function.query.Criteria.*;

import java.util.Arrays;

import org.junit.Test;

import org.springframework.data.r2dbc.function.query.Criteria.*;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Mark Paluch
 */
public class CriteriaUnitTests {

	@Test // gh-64
	public void andChainedCriteria() {

		Criteria criteria = of("foo").is("bar").and("baz").isNotNull();

		assertThat(criteria.getProperty()).isEqualTo("baz");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
		assertThat(criteria.getValue()).isNull();
		assertThat(criteria.getPrevious()).isNotNull();
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void orChainedCriteria() {

		Criteria criteria = of("foo").is("bar").or("baz").isNotNull();

		assertThat(criteria.getProperty()).isEqualTo("baz");
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria.getPrevious()).isNull();
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildEqualsCriteria() {

		Criteria criteria = of("foo").is("bar");

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildNotEqualsCriteria() {

		Criteria criteria = of("foo").not("bar");

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NEQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildInCriteria() {

		Criteria criteria = of("foo").in("bar", "baz");

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	public void shouldBuildNotInCriteria() {

		Criteria criteria = of("foo").notIn("bar", "baz");

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NOT_IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	public void shouldBuildGtCriteria() {

		Criteria criteria = of("foo").greaterThan(1);

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildGteCriteria() {

		Criteria criteria = of("foo").greaterThanOrEquals(1);

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLtCriteria() {

		Criteria criteria = of("foo").lessThan(1);

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLteCriteria() {

		Criteria criteria = of("foo").lessThanOrEquals(1);

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLikeCriteria() {

		Criteria criteria = of("foo").like("hello%");

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test // gh-64
	public void shouldBuildIsNullCriteria() {

		Criteria criteria = of("foo").isNull();

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NULL);
	}

	@Test // gh-64
	public void shouldBuildIsNotNullCriteria() {

		Criteria criteria = of("foo").isNotNull();

		assertThat(criteria.getProperty()).isEqualTo("foo");
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
	}
}
