/*
 * Copyright 2020-2024 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Interface defining a criteria definition object. A criteria definition may chain multiple predicates and may also
 * represent a group of nested criteria objects.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
public interface CriteriaDefinition {

	/**
	 * Static factory method to create an empty {@link CriteriaDefinition}.
	 *
	 * @return an empty {@link CriteriaDefinition}.
	 */
	static CriteriaDefinition empty() {
		return Criteria.EMPTY;
	}

	/**
	 * Create a new {@link CriteriaDefinition} and combine it as group with {@code AND} using the provided {@link List
	 * Criterias}.
	 *
	 * @return new {@link CriteriaDefinition}.
	 */
	static CriteriaDefinition from(CriteriaDefinition... criteria) {

		Assert.notNull(criteria, "Criteria must not be null");
		Assert.noNullElements(criteria, "Criteria must not contain null elements");

		return from(Arrays.asList(criteria));
	}

	/**
	 * Create a new {@link CriteriaDefinition} and combine it as group with {@code AND} using the provided {@link List
	 * Criterias}.
	 *
	 * @return new {@link CriteriaDefinition}.
	 * @since 1.1
	 */
	static CriteriaDefinition from(List<? extends CriteriaDefinition> criteria) {

		Assert.notNull(criteria, "Criteria must not be null");
		Assert.noNullElements(criteria, "Criteria must not contain null elements");

		if (criteria.isEmpty()) {
			return Criteria.EMPTY;
		}

		if (criteria.size() == 1) {
			return criteria.get(0);
		}

		return Criteria.EMPTY.and(criteria);
	}

	/**
	 * @return {@literal true} if this {@link Criteria} is empty.
	 */
	boolean isGroup();

	List<CriteriaDefinition> getGroup();

	/**
	 * @return the column/property name.
	 */
	@Nullable
	SqlIdentifier getColumn();

	/**
	 * @return {@link Criteria.Comparator}.
	 */
	@Nullable
	Comparator getComparator();

	/**
	 * @return the comparison value. Can be {@literal null}.
	 */
	@Nullable
	Object getValue();

	/**
	 * Checks whether comparison should be done in case-insensitive way.
	 *
	 * @return {@literal true} if comparison should be done in case-insensitive way
	 */
	boolean isIgnoreCase();

	/**
	 * @return the previous {@link CriteriaDefinition} object. Can be {@literal null} if there is no previous
	 *         {@link CriteriaDefinition}.
	 * @see #hasPrevious()
	 */
	@Nullable
	CriteriaDefinition getPrevious();

	/**
	 * @return {@literal true} if this {@link Criteria} has a previous one.
	 */
	boolean hasPrevious();

	/**
	 * @return {@literal true} if this {@link Criteria} is empty.
	 */
	boolean isEmpty();

	/**
	 * @return {@link Combinator} to combine this criteria with a previous one.
	 */
	Combinator getCombinator();

	enum Combinator {
		INITIAL, AND, OR;
	}

	enum Comparator {
		INITIAL(""), EQ("="), NEQ("!="), BETWEEN("BETWEEN"), NOT_BETWEEN("NOT BETWEEN"), LT("<"), LTE("<="), GT(">"), GTE(
				">="), IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL"), LIKE(
						"LIKE"), NOT_LIKE("NOT LIKE"), NOT_IN("NOT IN"), IN("IN"), IS_TRUE("IS TRUE"), IS_FALSE("IS FALSE");

		private final String comparator;

		Comparator(String comparator) {
			this.comparator = comparator;
		}

		public String getComparator() {
			return comparator;
		}
	}
}
