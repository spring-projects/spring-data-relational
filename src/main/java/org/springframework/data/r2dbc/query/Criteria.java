/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Central class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the {@code Criteria.property(…)} method will improve readability as in
 * {@code where(property(…).is(…)}.
 * <p>
 * The Criteria API supports composition with a {@link #empty() NULL object} and a {@link #from(List) static factory
 * method}. Example usage:
 *
 * <pre class="code">
 * Criteria.from(Criteria.where("name").is("Foo"), Criteria.from(Criteria.where("age").greaterThan(42)));
 * </pre>
 *
 * rendering:
 *
 * <pre class="code">
 * WHERE name = 'Foo' AND age > 42
 * </pre>
 *
 * @author Mark Paluch
 * @author Oliver Drotbohm
 */
public class Criteria {

	private static final Criteria EMPTY = new Criteria(SqlIdentifier.EMPTY, Comparator.INITIAL, null);

	private final @Nullable Criteria previous;
	private final Combinator combinator;
	private final List<Criteria> group;

	private final @Nullable SqlIdentifier column;
	private final @Nullable Comparator comparator;
	private final @Nullable Object value;

	private Criteria(SqlIdentifier column, Comparator comparator, @Nullable Object value) {
		this(null, Combinator.INITIAL, Collections.emptyList(), column, comparator, value);
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, List<Criteria> group,
			@Nullable SqlIdentifier column, @Nullable Comparator comparator, @Nullable Object value) {

		this.previous = previous;
		this.combinator = previous != null && previous.isEmpty() ? Combinator.INITIAL : combinator;
		this.group = group;
		this.column = column;
		this.comparator = comparator;
		this.value = value;
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, List<Criteria> group) {

		this.previous = previous;
		this.combinator = previous != null && previous.isEmpty() ? Combinator.INITIAL : combinator;
		this.group = group;
		this.column = null;
		this.comparator = null;
		this.value = null;
	}

	/**
	 * Static factory method to create an empty Criteria.
	 *
	 * @return an empty {@link Criteria}.
	 * @since 1.1
	 */
	public static Criteria empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link List Criterias}.
	 *
	 * @return new {@link Criteria}.
	 * @since 1.1
	 */
	public static Criteria from(Criteria... criteria) {

		Assert.notNull(criteria, "Criteria must not be null");
		Assert.noNullElements(criteria, "Criteria must not contain null elements");

		return from(Arrays.asList(criteria));
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link List Criterias}.
	 *
	 * @return new {@link Criteria}.
	 * @since 1.1
	 */
	public static Criteria from(List<Criteria> criteria) {

		Assert.notNull(criteria, "Criteria must not be null");
		Assert.noNullElements(criteria, "Criteria must not contain null elements");

		if (criteria.isEmpty()) {
			return EMPTY;
		}

		if (criteria.size() == 1) {
			return criteria.get(0);
		}

		return EMPTY.and(criteria);
	}

	/**
	 * Static factory method to create a Criteria using the provided {@code column} name.
	 *
	 * @param column Must not be {@literal null} or empty.
	 * @return a new {@link CriteriaStep} object to complete the first {@link Criteria}.
	 */
	public static CriteriaStep where(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		return new DefaultCriteriaStep(SqlIdentifier.unquoted(column));
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code AND} using the provided {@code column} name.
	 *
	 * @param column Must not be {@literal null} or empty.
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep and(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		SqlIdentifier identifier = SqlIdentifier.unquoted(column);
		return new DefaultCriteriaStep(identifier) {
			@Override
			protected Criteria createCriteria(Comparator comparator, Object value) {
				return new Criteria(Criteria.this, Combinator.AND, Collections.emptyList(), identifier, comparator, value);
			}
		};
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria object.
	 * @return a new {@link Criteria} object.
	 * @since 1.1
	 */
	public Criteria and(Criteria criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");

		return and(Collections.singletonList(criteria));
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria objects.
	 * @return a new {@link Criteria} object.
	 * @since 1.1
	 */
	public Criteria and(List<Criteria> criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");

		return new Criteria(Criteria.this, Combinator.AND, criteria);
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code OR} using the provided {@code column} name.
	 *
	 * @param column Must not be {@literal null} or empty.
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep or(String column) {

		Assert.hasText(column, "Column name must not be null or empty!");

		SqlIdentifier identifier = SqlIdentifier.unquoted(column);
		return new DefaultCriteriaStep(identifier) {
			@Override
			protected Criteria createCriteria(Comparator comparator, Object value) {
				return new Criteria(Criteria.this, Combinator.OR, Collections.emptyList(), identifier, comparator, value);
			}
		};
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code OR} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria object.
	 * @return a new {@link Criteria} object.
	 * @since 1.1
	 */
	public Criteria or(Criteria criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");

		return or(Collections.singletonList(criteria));
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code OR} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria object.
	 * @return a new {@link Criteria} object.
	 * @since 1.1
	 */
	public Criteria or(List<Criteria> criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");

		return new Criteria(Criteria.this, Combinator.OR, criteria);
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
	 * @return {@literal true} if this {@link Criteria} is empty.
	 * @since 1.1
	 */
	public boolean isEmpty() {

		if (!doIsEmpty()) {
			return false;
		}

		Criteria parent = this.previous;

		while (parent != null) {

			if (!parent.doIsEmpty()) {
				return false;
			}

			parent = parent.previous;
		}

		return true;
	}

	private boolean doIsEmpty() {

		if (this.comparator == Comparator.INITIAL) {
			return true;
		}

		if (this.column != null) {
			return false;
		}

		for (Criteria criteria : group) {
			if (!criteria.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @return {@literal true} if this {@link Criteria} is empty.
	 */
	boolean isGroup() {
		return !this.group.isEmpty();
	}

	/**
	 * @return {@link Combinator} to combine this criteria with a previous one.
	 */
	Combinator getCombinator() {
		return combinator;
	}

	List<Criteria> getGroup() {
		return group;
	}

	/**
	 * @return the column/property name.
	 */
	@Nullable
	SqlIdentifier getColumn() {
		return column;
	}

	/**
	 * @return {@link Comparator}.
	 */
	@Nullable
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
		INITIAL, EQ, NEQ, LT, LTE, GT, GTE, IS_NULL, IS_NOT_NULL, LIKE, NOT_IN, IN,
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
		 * @param value must not be {@literal null}.
		 */
		Criteria is(Object value);

		/**
		 * Creates a {@link Criteria} using equality (is not).
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria not(Object value);

		/**
		 * Creates a {@link Criteria} using {@code IN}.
		 *
		 * @param values must not be {@literal null}.
		 */
		Criteria in(Object... values);

		/**
		 * Creates a {@link Criteria} using {@code IN}.
		 *
		 * @param values must not be {@literal null}.
		 */
		Criteria in(Collection<?> values);

		/**
		 * Creates a {@link Criteria} using {@code NOT IN}.
		 *
		 * @param values must not be {@literal null}.
		 */
		Criteria notIn(Object... values);

		/**
		 * Creates a {@link Criteria} using {@code NOT IN}.
		 *
		 * @param values must not be {@literal null}.
		 */
		Criteria notIn(Collection<?> values);

		/**
		 * Creates a {@link Criteria} using less-than ({@literal <}).
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria lessThan(Object value);

		/**
		 * Creates a {@link Criteria} using less-than or equal to ({@literal <=}).
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria lessThanOrEquals(Object value);

		/**
		 * Creates a {@link Criteria} using greater-than({@literal >}).
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria greaterThan(Object value);

		/**
		 * Creates a {@link Criteria} using greater-than or equal to ({@literal >=}).
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria greaterThanOrEquals(Object value);

		/**
		 * Creates a {@link Criteria} using {@code LIKE}.
		 *
		 * @param value must not be {@literal null}.
		 */
		Criteria like(Object value);

		/**
		 * Creates a {@link Criteria} using {@code IS NULL}.
		 */
		Criteria isNull();

		/**
		 * Creates a {@link Criteria} using {@code IS NOT NULL}.
		 */
		Criteria isNotNull();
	}

	/**
	 * Default {@link CriteriaStep} implementation.
	 */
	static class DefaultCriteriaStep implements CriteriaStep {

		private final SqlIdentifier property;

		DefaultCriteriaStep(SqlIdentifier property) {
			this.property = property;
		}

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
			Assert.noNullElements(values, "Values must not contain a null value!");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.IN, Arrays.asList(values));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#in(java.util.Collection)
		 */
		@Override
		public Criteria in(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			Assert.noNullElements(values.toArray(), "Values must not contain a null value!");

			return createCriteria(Comparator.IN, values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#notIn(java.lang.Object[])
		 */
		@Override
		public Criteria notIn(Object... values) {

			Assert.notNull(values, "Values must not be null!");
			Assert.noNullElements(values, "Values must not contain a null value!");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.NOT_IN, Arrays.asList(values));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.query.Criteria.CriteriaStep#notIn(java.util.Collection)
		 */
		@Override
		public Criteria notIn(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			Assert.noNullElements(values.toArray(), "Values must not contain a null value!");

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
			return new Criteria(this.property, comparator, value);
		}
	}
}
