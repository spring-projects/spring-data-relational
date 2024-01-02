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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.Pair;
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
 * @author Roman Chigvintsev
 * @author Jens Schauder
 * @since 2.0
 */
public class Criteria implements CriteriaDefinition {

	static final Criteria EMPTY = new Criteria(SqlIdentifier.EMPTY, Comparator.INITIAL, null);

	private final @Nullable Criteria previous;
	private final Combinator combinator;
	private final List<CriteriaDefinition> group;

	private final @Nullable SqlIdentifier column;
	private final @Nullable Comparator comparator;
	private final @Nullable Object value;
	private final boolean ignoreCase;

	private Criteria(SqlIdentifier column, Comparator comparator, @Nullable Object value) {
		this(null, Combinator.INITIAL, Collections.emptyList(), column, comparator, value, false);
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, List<CriteriaDefinition> group,
			@Nullable SqlIdentifier column, @Nullable Comparator comparator, @Nullable Object value) {
		this(previous, combinator, group, column, comparator, value, false);
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, List<CriteriaDefinition> group,
			@Nullable SqlIdentifier column, @Nullable Comparator comparator, @Nullable Object value, boolean ignoreCase) {

		this.previous = previous;
		this.combinator = previous != null && previous.isEmpty() ? Combinator.INITIAL : combinator;
		this.group = group;
		this.column = column;
		this.comparator = comparator;
		this.value = value;
		this.ignoreCase = ignoreCase;
	}

	private Criteria(@Nullable Criteria previous, Combinator combinator, List<CriteriaDefinition> group) {

		this.previous = previous;
		this.combinator = previous != null && previous.isEmpty() ? Combinator.INITIAL : combinator;
		this.group = group;
		this.column = null;
		this.comparator = null;
		this.value = null;
		this.ignoreCase = false;
	}

	/**
	 * Static factory method to create an empty Criteria.
	 *
	 * @return an empty {@link Criteria}.
	 */
	public static Criteria empty() {
		return EMPTY;
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link List Criterias}.
	 *
	 * @return new {@link Criteria}.
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

		Assert.hasText(column, "Column name must not be null or empty");

		return new DefaultCriteriaStep(SqlIdentifier.unquoted(column));
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code AND} using the provided {@code column} name.
	 *
	 * @param column Must not be {@literal null} or empty.
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep and(String column) {

		Assert.hasText(column, "Column name must not be null or empty");

		SqlIdentifier identifier = SqlIdentifier.unquoted(column);
		return new DefaultCriteriaStep(identifier) {
			@Override
			protected Criteria createCriteria(Comparator comparator, @Nullable Object value) {
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
	public Criteria and(CriteriaDefinition criteria) {

		Assert.notNull(criteria, "Criteria must not be null");

		return and(Collections.singletonList(criteria));
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code AND} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria objects.
	 * @return a new {@link Criteria} object.
	 */
	@SuppressWarnings("unchecked")
	public Criteria and(List<? extends CriteriaDefinition> criteria) {

		Assert.notNull(criteria, "Criteria must not be null");

		return new Criteria(Criteria.this, Combinator.AND, (List<CriteriaDefinition>) criteria);
	}

	/**
	 * Create a new {@link Criteria} and combine it with {@code OR} using the provided {@code column} name.
	 *
	 * @param column Must not be {@literal null} or empty.
	 * @return a new {@link CriteriaStep} object to complete the next {@link Criteria}.
	 */
	public CriteriaStep or(String column) {

		Assert.hasText(column, "Column name must not be null or empty");

		SqlIdentifier identifier = SqlIdentifier.unquoted(column);
		return new DefaultCriteriaStep(identifier) {
			@Override
			protected Criteria createCriteria(Comparator comparator, @Nullable Object value) {
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
	public Criteria or(CriteriaDefinition criteria) {

		Assert.notNull(criteria, "Criteria must not be null");

		return or(Collections.singletonList(criteria));
	}

	/**
	 * Create a new {@link Criteria} and combine it as group with {@code OR} using the provided {@link Criteria} group.
	 *
	 * @param criteria criteria object.
	 * @return a new {@link Criteria} object.
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Criteria or(List<? extends CriteriaDefinition> criteria) {

		Assert.notNull(criteria, "Criteria must not be null");

		return new Criteria(Criteria.this, Combinator.OR, (List<CriteriaDefinition>) criteria);
	}

	/**
	 * Creates a new {@link Criteria} with the given "ignore case" flag.
	 *
	 * @param ignoreCase {@literal true} if comparison should be done in case-insensitive way
	 * @return a new {@link Criteria} object
	 */
	public Criteria ignoreCase(boolean ignoreCase) {
		if (this.ignoreCase != ignoreCase) {
			return new Criteria(previous, combinator, group, column, comparator, value, ignoreCase);
		}
		return this;
	}

	/**
	 * @return the previous {@link Criteria} object. Can be {@literal null} if there is no previous {@link Criteria}.
	 * @see #hasPrevious()
	 */
	@Nullable
	public Criteria getPrevious() {
		return previous;
	}

	/**
	 * @return {@literal true} if this {@link Criteria} has a previous one.
	 */
	public boolean hasPrevious() {
		return previous != null;
	}

	/**
	 * @return {@literal true} if this {@link Criteria} is empty.
	 * @since 1.1
	 */
	@Override
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

		for (CriteriaDefinition criteria : group) {

			if (!criteria.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @return {@literal true} if this {@link Criteria} is empty.
	 */
	public boolean isGroup() {
		return !this.group.isEmpty();
	}

	/**
	 * @return {@link Combinator} to combine this criteria with a previous one.
	 */
	public Combinator getCombinator() {
		return combinator;
	}

	@Override
	public List<CriteriaDefinition> getGroup() {
		return group;
	}

	/**
	 * @return the column/property name.
	 */
	@Nullable
	public SqlIdentifier getColumn() {
		return column;
	}

	/**
	 * @return {@link Comparator}.
	 */
	@Nullable
	public Comparator getComparator() {
		return comparator;
	}

	/**
	 * @return the comparison value. Can be {@literal null}.
	 */
	@Nullable
	public Object getValue() {
		return value;
	}

	/**
	 * Checks whether comparison should be done in case-insensitive way.
	 *
	 * @return {@literal true} if comparison should be done in case-insensitive way
	 */
	@Override
	public boolean isIgnoreCase() {
		return ignoreCase;
	}

	@Override
	public String toString() {

		if (isEmpty()) {
			return "";
		}

		StringBuilder builder = new StringBuilder();
		unroll(this, builder);

		return builder.toString();
	}

	private void unroll(CriteriaDefinition criteria, StringBuilder stringBuilder) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		render(current, stringBuilder);
		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);

			if (criterion.getCombinator() != Combinator.INITIAL) {
				stringBuilder.append(' ').append(criterion.getCombinator().name()).append(' ');
			}

			render(criterion, stringBuilder);

			current = criterion;
		}
	}

	private void unrollGroup(List<? extends CriteriaDefinition> criteria, StringBuilder stringBuilder) {

		stringBuilder.append("(");

		boolean first = true;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			if (!first) {
				Combinator combinator = criterion.getCombinator() == Combinator.INITIAL ? Combinator.AND
						: criterion.getCombinator();
				stringBuilder.append(' ').append(combinator.name()).append(' ');
			}

			unroll(criterion, stringBuilder);
			first = false;
		}

		stringBuilder.append(")");
	}

	private void render(CriteriaDefinition criteria, StringBuilder stringBuilder) {

		if (criteria.isEmpty()) {
			return;
		}

		if (criteria.isGroup()) {
			unrollGroup(criteria.getGroup(), stringBuilder);
			return;
		}

		stringBuilder.append(criteria.getColumn().toSql(IdentifierProcessing.NONE)).append(' ')
				.append(criteria.getComparator().getComparator());

		switch (criteria.getComparator()) {
			case BETWEEN:
			case NOT_BETWEEN:
				Pair<Object, Object> pair = (Pair<Object, Object>) criteria.getValue();
				stringBuilder.append(' ').append(pair.getFirst()).append(" AND ").append(pair.getSecond());
				break;

			case IS_NULL:
			case IS_NOT_NULL:
			case IS_TRUE:
			case IS_FALSE:
				break;

			case IN:
			case NOT_IN:
				stringBuilder.append(" (").append(renderValue(criteria.getValue())).append(')');
				break;

			default:
				stringBuilder.append(' ').append(renderValue(criteria.getValue()));
		}
	}

	private static String renderValue(@Nullable Object value) {

		if (value instanceof Number) {
			return value.toString();
		}

		if (value instanceof Collection) {

			StringJoiner joiner = new StringJoiner(", ");
			((Collection<?>) value).forEach(o -> joiner.add(renderValue(o)));
			return joiner.toString();
		}

		if (value != null) {
			return String.format("'%s'", value);
		}

		return "null";
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
		 * Creates a {@link Criteria} using between ({@literal BETWEEN begin AND end}).
		 *
		 * @param begin must not be {@literal null}.
		 * @param end must not be {@literal null}.
		 * @since 2.2
		 */
		Criteria between(Object begin, Object end);

		/**
		 * Creates a {@link Criteria} using not between ({@literal NOT BETWEEN begin AND end}).
		 *
		 * @param begin must not be {@literal null}.
		 * @param end must not be {@literal null}.
		 * @since 2.2
		 */
		Criteria notBetween(Object begin, Object end);

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
		 * Creates a {@link Criteria} using {@code NOT LIKE}.
		 *
		 * @param value must not be {@literal null}
		 * @return a new {@link Criteria} object
		 */
		Criteria notLike(Object value);

		/**
		 * Creates a {@link Criteria} using {@code IS NULL}.
		 */
		Criteria isNull();

		/**
		 * Creates a {@link Criteria} using {@code IS NOT NULL}.
		 */
		Criteria isNotNull();

		/**
		 * Creates a {@link Criteria} using {@code IS TRUE}.
		 *
		 * @return a new {@link Criteria} object
		 */
		Criteria isTrue();

		/**
		 * Creates a {@link Criteria} using {@code IS FALSE}.
		 *
		 * @return a new {@link Criteria} object
		 */
		Criteria isFalse();
	}

	/**
	 * Default {@link CriteriaStep} implementation.
	 */
	static class DefaultCriteriaStep implements CriteriaStep {

		private final SqlIdentifier property;

		DefaultCriteriaStep(SqlIdentifier property) {
			this.property = property;
		}

		@Override
		public Criteria is(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.EQ, value);
		}

		@Override
		public Criteria not(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.NEQ, value);
		}

		@Override
		public Criteria in(Object... values) {

			Assert.notNull(values, "Values must not be null");
			Assert.noNullElements(values, "Values must not contain a null value");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.IN, Arrays.asList(values));
		}

		@Override
		public Criteria in(Collection<?> values) {

			Assert.notNull(values, "Values must not be null");
			Assert.noNullElements(values.toArray(), "Values must not contain a null value");

			return createCriteria(Comparator.IN, values);
		}

		@Override
		public Criteria notIn(Object... values) {

			Assert.notNull(values, "Values must not be null");
			Assert.noNullElements(values, "Values must not contain a null value");

			if (values.length > 1 && values[1] instanceof Collection) {
				throw new InvalidDataAccessApiUsageException(
						"You can only pass in one argument of type " + values[1].getClass().getName());
			}

			return createCriteria(Comparator.NOT_IN, Arrays.asList(values));
		}

		@Override
		public Criteria notIn(Collection<?> values) {

			Assert.notNull(values, "Values must not be null");
			Assert.noNullElements(values.toArray(), "Values must not contain a null value");

			return createCriteria(Comparator.NOT_IN, values);
		}

		@Override
		public Criteria between(Object begin, Object end) {

			Assert.notNull(begin, "Begin value must not be null");
			Assert.notNull(end, "End value must not be null");

			return createCriteria(Comparator.BETWEEN, Pair.of(begin, end));
		}

		@Override
		public Criteria notBetween(Object begin, Object end) {

			Assert.notNull(begin, "Begin value must not be null");
			Assert.notNull(end, "End value must not be null");

			return createCriteria(Comparator.NOT_BETWEEN, Pair.of(begin, end));
		}

		@Override
		public Criteria lessThan(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.LT, value);
		}

		@Override
		public Criteria lessThanOrEquals(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.LTE, value);
		}

		@Override
		public Criteria greaterThan(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.GT, value);
		}

		@Override
		public Criteria greaterThanOrEquals(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.GTE, value);
		}

		@Override
		public Criteria like(Object value) {

			Assert.notNull(value, "Value must not be null");

			return createCriteria(Comparator.LIKE, value);
		}

		@Override
		public Criteria notLike(Object value) {
			Assert.notNull(value, "Value must not be null");
			return createCriteria(Comparator.NOT_LIKE, value);
		}

		@Override
		public Criteria isNull() {
			return createCriteria(Comparator.IS_NULL, null);
		}

		@Override
		public Criteria isNotNull() {
			return createCriteria(Comparator.IS_NOT_NULL, null);
		}

		@Override
		public Criteria isTrue() {
			return createCriteria(Comparator.IS_TRUE, true);
		}

		@Override
		public Criteria isFalse() {
			return createCriteria(Comparator.IS_FALSE, false);
		}

		protected Criteria createCriteria(Comparator comparator, @Nullable Object value) {
			return new Criteria(this.property, comparator, value);
		}
	}
}
