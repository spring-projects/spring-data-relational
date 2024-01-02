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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * SqlSort supports additional to {@link Sort} {@literal unsafe} sort expressions. Such sort expressions get included in
 * a query as they are. The user has to ensure that they come from trusted sorted or are properly sanatized to prevent
 * SQL injection attacks.
 * 
 * @author Jens Schauder
 * @since 3.1
 */
public class SqlSort extends Sort {

	private static final Predicate<String> predicate = Pattern.compile("^[0-9a-zA-Z_\\.\\(\\)]*$").asPredicate();

	private static final long serialVersionUID = 1L;

	private SqlSort(Direction direction, List<String> paths) {
		this(Collections.<Order> emptyList(), direction, paths);
	}

	private SqlSort(List<Order> orders, @Nullable Direction direction, List<String> paths) {
		super(combine(orders, direction, paths));
	}

	private SqlSort(List<Order> orders) {
		super(orders);
	}

	/**
	 * @param paths must not be {@literal null} or empty.
	 */
	public static SqlSort of(String... paths) {
		return new SqlSort(DEFAULT_DIRECTION, Arrays.asList(paths));
	}

	/**
	 * @param direction the sorting direction.
	 * @param paths must not be {@literal null} or empty.
	 */
	public static SqlSort of(Direction direction, String... paths) {
		return new SqlSort(direction, Arrays.asList(paths));
	}

	/**
	 * Validates a {@link org.springframework.data.domain.Sort.Order}, to be either safe for use in SQL or to be
	 * explicitely marked unsafe.
	 * 
	 * @param order the {@link org.springframework.data.domain.Sort.Order} to validate. Must not be null.
	 */
	public static void validate(Sort.Order order) {

		String property = order.getProperty();
		boolean isMarkedUnsafe = order instanceof SqlSort.SqlOrder ro && ro.isUnsafe();
		if (isMarkedUnsafe) {
			return;
		}

		if (!predicate.test(property)) {
			throw new IllegalArgumentException(
					"order fields that are not marked as unsafe must only consist of digits, letter, '.', '_', and '\'. If you want to sort by arbitrary expressions please use RelationalSort.unsafe. Note that such expressions become part of SQL statements and therefore need to be sanatized to prevent SQL injection attacks.");
		}
	}

	private static List<Order> combine(List<Order> orders, @Nullable Direction direction, List<String> paths) {

		List<Order> result = new ArrayList<>(orders);

		for (String path : paths) {
			result.add(new Order(direction, path));
		}

		return result;
	}

	/**
	 * Creates new unsafe {@link SqlSort} based on given properties.
	 *
	 * @param properties must not be {@literal null} or empty.
	 * @return
	 */
	public static SqlSort unsafe(String... properties) {
		return unsafe(Sort.DEFAULT_DIRECTION, properties);
	}

	/**
	 * Creates new unsafe {@link SqlSort} based on given {@link Direction} and properties.
	 *
	 * @param direction must not be {@literal null}.
	 * @param properties must not be {@literal null} or empty.
	 * @return
	 */
	public static SqlSort unsafe(Direction direction, String... properties) {

		Assert.notNull(direction, "Direction must not be null");
		Assert.notEmpty(properties, "Properties must not be empty");
		Assert.noNullElements(properties, "Properties must not contain null values");

		return unsafe(direction, Arrays.asList(properties));
	}

	/**
	 * Creates new unsafe {@link SqlSort} based on given {@link Direction} and properties.
	 *
	 * @param direction must not be {@literal null}.
	 * @param properties must not be {@literal null} or empty.
	 * @return
	 */
	public static SqlSort unsafe(Direction direction, List<String> properties) {

		Assert.notEmpty(properties, "Properties must not be empty");

		List<Order> orders = new ArrayList<>(properties.size());

		for (String property : properties) {
			orders.add(new SqlOrder(direction, property));
		}

		return new SqlSort(orders);
	}

	/**
	 * Returns a new {@link SqlSort} with the given sorting criteria added to the current one.
	 *
	 * @param direction can be {@literal null}.
	 * @param paths must not be {@literal null}.
	 * @return
	 */
	public SqlSort and(@Nullable Direction direction, String... paths) {

		Assert.notNull(paths, "Paths must not be null");

		List<Order> existing = new ArrayList<>();

		for (Order order : this) {
			existing.add(order);
		}

		return new SqlSort(existing, direction, Arrays.asList(paths));
	}

	/**
	 * Returns a new {@link SqlSort} with the given sorting criteria added to the current one.
	 *
	 * @param direction can be {@literal null}.
	 * @param properties must not be {@literal null} or empty.
	 * @return
	 */
	public SqlSort andUnsafe(@Nullable Direction direction, String... properties) {

		Assert.notEmpty(properties, "Properties must not be empty");

		List<Order> orders = new ArrayList<>();

		for (Order order : this) {
			orders.add(order);
		}

		for (String property : properties) {
			orders.add(new SqlOrder(direction, property));
		}

		return new SqlSort(orders, direction, Collections.emptyList());
	}

	/**
	 * Custom {@link Order} that keeps a flag to indicate unsafe property handling, i.e. the String provided is not
	 * necessarily a property but can be an arbitrary expression piped into the query execution. We also keep an
	 * additional {@code ignoreCase} flag around as the constructor of the superclass is private currently.
	 *
	 * @author Christoph Strobl
	 * @author Oliver Gierke
	 */
	public static class SqlOrder extends Order {

		private static final long serialVersionUID = 1L;

		private final boolean unsafe;

		/**
		 * Creates a new {@link SqlOrder} instance. Takes a single property. Direction defaults to
		 * {@link Sort#DEFAULT_DIRECTION}.
		 *
		 * @param property must not be {@literal null} or empty.
		 */
		public static SqlOrder by(String property) {
			return new SqlOrder(DEFAULT_DIRECTION, property);
		}

		/**
		 * Creates a new {@link SqlOrder} instance. Takes a single property. Direction is {@link Direction#ASC} and
		 * NullHandling {@link NullHandling#NATIVE}.
		 *
		 * @param property must not be {@literal null} or empty.
		 */
		public static SqlOrder asc(String property) {
			return new SqlOrder(Direction.ASC, property, NullHandling.NATIVE);
		}

		/**
		 * Creates a new {@link SqlOrder} instance. Takes a single property. Direction is {@link Direction#DESC} and
		 * NullHandling {@link NullHandling#NATIVE}.
		 *
		 * @param property must not be {@literal null} or empty.
		 */
		public static SqlOrder desc(String property) {
			return new SqlOrder(Direction.DESC, property, NullHandling.NATIVE);
		}

		/**
		 * Creates a new {@link SqlOrder} instance. if order is {@literal null} then order defaults to
		 * {@link Sort#DEFAULT_DIRECTION}
		 *
		 * @param direction can be {@literal null}, will default to {@link Sort#DEFAULT_DIRECTION}.
		 * @param property must not be {@literal null}.
		 */
		private SqlOrder(@Nullable Direction direction, String property) {
			this(direction, property, NullHandling.NATIVE);
		}

		/**
		 * Creates a new {@link SqlOrder} instance. if order is {@literal null} then order defaults to
		 * {@link Sort#DEFAULT_DIRECTION}.
		 *
		 * @param direction can be {@literal null}, will default to {@link Sort#DEFAULT_DIRECTION}.
		 * @param property must not be {@literal null}.
		 * @param nullHandlingHint can be {@literal null}, will default to {@link NullHandling#NATIVE}.
		 */
		private SqlOrder(@Nullable Direction direction, String property, NullHandling nullHandlingHint) {
			this(direction, property, nullHandlingHint, false, true);
		}

		private SqlOrder(@Nullable Direction direction, String property, NullHandling nullHandling, boolean ignoreCase,
				boolean unsafe) {

			super(direction, property, ignoreCase, nullHandling);
			this.unsafe = unsafe;
		}

		@Override
		public SqlOrder with(Direction order) {
			return new SqlOrder(order, getProperty(), getNullHandling(), isIgnoreCase(), isUnsafe());
		}

		@Override
		public SqlOrder with(NullHandling nullHandling) {
			return new SqlOrder(getDirection(), getProperty(), nullHandling, isIgnoreCase(), isUnsafe());
		}

		public SqlOrder withUnsafe() {
			return new SqlOrder(getDirection(), getProperty(), getNullHandling(), isIgnoreCase(), true);
		}

		@Override
		public SqlOrder ignoreCase() {
			return new SqlOrder(getDirection(), getProperty(), getNullHandling(), true, isUnsafe());
		}

		/**
		 * @return true if {@link SqlOrder} should not be validated automatically. The validation should be done by the
		 *         developer using this.
		 */
		public boolean isUnsafe() {
			return unsafe;
		}
	}
}
