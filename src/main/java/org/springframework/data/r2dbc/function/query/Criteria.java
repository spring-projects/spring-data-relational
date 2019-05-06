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

import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Central class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the {@code Criteria.property(…)} method will improve readability as in
 * {@code where(property(…).is(…)}.
 *
 * @author Mark Paluch
 */
public class Criteria {

	private final @Nullable Criteria previous;
	private final Combinator combinator;

	private final String column;
	private final Comparator comparator;
	private final @Nullable Object value;

	private Criteria(String column, Comparator comparator, @Nullable Object value) {
		this(null, Combinator.INITIAL, column, comparator, value);
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, String column, Comparator comparator,
			@Nullable Object value) {
		this.previous = previous;
		this.combinator = combinator;
		this.column = column;
		this.comparator = comparator;
		this.value = value;
	}

	/**
	 * Static factory method to create a Criteria using the provided {@code column} name.
	 *
	 * @param column
	 * @return a new {@link CriteriaStep} object to complete the first {@link Criteria}.
	 */
	public static CriteriaStep where(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		return new DefaultCriteriaStep(column);
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code AND} using the provided {@code column} name.
	 *
	 * @param column
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep and(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		return new DefaultCriteriaStep(column) {
			@Override
			protected Criteria createCriteria(Comparator comparator, Object value) {
				return new Criteria(Criteria.this, Combinator.AND, column, comparator, value);
			}
		};
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code OR} using the provided {@code column} name.
	 *
	 * @param column
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep or(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		return new DefaultCriteriaStep(column) {
			@Override
			protected Criteria createCriteria(Comparator comparator, Object value) {
				return new Criteria(Criteria.this, Combinator.OR, column, comparator, value);
			}
		};
	}

	/**
	 * @return the previous {@link Criteria} object. Can be {@literal null} if there is no previous {@link Criteria}.
	 * @see #hasPrevious()
	 */
	@Nullable
	Criteria getPrevious() {
		return previous;
	}

	/**
	 * @return {@literal true} if this {@link Criteria} has a previous one.
	 */
	boolean hasPrevious() {
		return previous != null;
	}

	/**
	 * @return {@link Combinator} to combine this criteria with a previous one.
	 */
	Combinator getCombinator() {
		return combinator;
	}

	/**
	 * @return the property name.
	 */
	String getColumn() {
		return column;
	}

	/**
	 * @return {@link Comparator}.
	 */
	Comparator getComparator() {
		return comparator;
	}

	/**
	 * @return the comparison value. Can be {@literal null}.
	 */
	@Nullable
	Object getValue() {
		return value;
	}

	enum Comparator {
		EQ, NEQ, LT, LTE, GT, GTE, IS_NULL, IS_NOT_NULL, LIKE, NOT_IN, IN,
	}

	enum Combinator {
		INITIAL, AND, OR;
	}

	/**
	 * Interface declaring terminal builder methods to build a {@link Criteria}.
	 */
	public interface CriteriaStep {

		/**
		 * Creates a {@link Criteria} using equality.
		 *
		 * @param value
		 * @return
		 */
		Criteria is(Object value);

		/**
		 * Creates a {@link Criteria} using equality (is not).
		 *
		 * @param value
		 * @return
		 */
		Criteria not(Object value);

		/**
		 * Creates a {@link Criteria} using {@code IN}.
		 *
		 * @param value
		 * @return
		 */
		Criteria in(Object... values);

		/**
		 * Creates a {@link Criteria} using {@code IN}.
		 *
		 * @param value
		 * @return
		 */
		Criteria in(Collection<? extends Object> values);

		/**
		 * Creates a {@link Criteria} using {@code NOT IN}.
		 *
		 * @param value
		 * @return
		 */
		Criteria notIn(Object... values);

		/**
		 * Creates a {@link Criteria} using {@code NOT IN}.
		 *
		 * @param value
		 * @return
		 */
		Criteria notIn(Collection<? extends Object> values);

		/**
		 * Creates a {@link Criteria} using less-than ({@literal <}).
		 *
		 * @param value
		 * @return
		 */
		Criteria lessThan(Object value);

		/**
		 * Creates a {@link Criteria} using less-than or equal to ({@literal <=}).
		 *
		 * @param value
		 * @return
		 */
		Criteria lessThanOrEquals(Object value);

		/**
		 * Creates a {@link Criteria} using greater-than({@literal >}).
		 *
		 * @param value
		 * @return
		 */
		Criteria greaterThan(Object value);

		/**
		 * Creates a {@link Criteria} using greater-than or equal to ({@literal >=}).
		 *
		 * @param value
		 * @return
		 */
		Criteria greaterThanOrEquals(Object value);

		/**
		 * Creates a {@link Criteria} using {@code LIKE}.
		 *
		 * @param value
		 * @return
		 */
		Criteria like(Object value);

		/**
		 * Creates a {@link Criteria} using {@code IS NULL}.
		 *
		 * @param value
		 * @return
		 */
		Criteria isNull();

		/**
		 * Creates a {@link Criteria} using {@code IS NOT NULL}.
		 *
		 * @param value
		 * @return
		 */
		Criteria isNotNull();
	}

	/**
	 * Default {@link CriteriaStep} implementation.
	 */
	@RequiredArgsConstructor
	static class DefaultCriteriaStep implements CriteriaStep {

		private final String property;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#is(java.lang.Object)
		 */
		@Override
		public Criteria is(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.EQ, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#not(java.lang.Object)
		 */
		@Override
		public Criteria not(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.NEQ, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#in(java.lang.Object[])
		 */
		@Override
		public Criteria in(Object... values) {

			Assert.notNull(values, "Values must not be null!");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.IN, Arrays.asList(values));
		}

		/**
		 * @param values
		 * @return
		 */
		@Override
		public Criteria in(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");

			return createCriteria(Comparator.IN, values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#notIn(java.lang.Object[])
		 */
		@Override
		public Criteria notIn(Object... values) {

			Assert.notNull(values, "Values must not be null!");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.NOT_IN, Arrays.asList(values));
		}

		/**
		 * @param values
		 * @return
		 */
		@Override
		public Criteria notIn(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");

			return createCriteria(Comparator.NOT_IN, values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#lessThan(java.lang.Object)
		 */
		@Override
		public Criteria lessThan(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.LT, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#lessThanOrEquals(java.lang.Object)
		 */
		@Override
		public Criteria lessThanOrEquals(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.LTE, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#greaterThan(java.lang.Object)
		 */
		@Override
		public Criteria greaterThan(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.GT, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#greaterThanOrEquals(java.lang.Object)
		 */
		@Override
		public Criteria greaterThanOrEquals(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.GTE, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#like(java.lang.Object)
		 */
		@Override
		public Criteria like(Object value) {

			Assert.notNull(value, "Value must not be null!");

			return createCriteria(Comparator.LIKE, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#isNull()
		 */
		@Override
		public Criteria isNull() {
			return createCriteria(Comparator.IS_NULL, null);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#isNotNull()
		 */
		@Override
		public Criteria isNotNull() {
			return createCriteria(Comparator.IS_NOT_NULL, null);
		}

		protected Criteria createCriteria(Comparator comparator, Object value) {
			return new Criteria(property, comparator, value);
		}
	}
}
