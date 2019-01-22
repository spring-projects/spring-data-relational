/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.util.Assert;

/**
 * Factory for common {@link Expression function expressions}.
 *
 * @author Mark Paluch
 * @see SQL
 * @see Expressions
 * @see Functions
 */
public class Functions {

	/**
	 * Creates a new {@link Distinct} function.
	 *
	 * @param columnNames column names to apply distinction, must not be {@literal null}.
	 * @return the new {@link Distinct} for {@code columns}.
	 */
	public static Distinct distinct(String... columnNames) {

		Assert.notNull(columnNames, "Columns must not be null!");

		List<Column> columns = new ArrayList<>();
		for (String columnName : columnNames) {
			columns.add(Column.create(columnName));
		}

		return distinct(columns);
	}

	/**
	 * Creates a new {@link Distinct} function.
	 *
	 * @param columns columns to apply distinction, must not be {@literal null}.
	 * @return the new {@link Distinct} for {@code columns}.
	 */
	public static Distinct distinct(Column... columns) {

		Assert.notNull(columns, "Columns must not be null!");

		return new Distinct(Arrays.asList(columns));
	}

	/**
	 * Creates a new {@link Distinct} function.
	 *
	 * @param columns columns to apply distinction, must not be {@literal null}.
	 * @return the new {@link Distinct} for {@code columns}.
	 */
	public static Distinct distinct(Collection<? extends Column> columns) {

		Assert.notNull(columns, "Columns must not be null!");

		return new Distinct(new ArrayList<>(columns));
	}

	/**
	 * Creates a new {@code COUNT} function for a single {@code column}.
	 *
	 * @param column column to apply count, must not be {@literal null} or empty.
	 * @return the new {@link SimpleFunction count function} for {@code column}.
	 */
	public static SimpleFunction count(String column) {
		return count(Column.create(column));
	}

	/**
	 * Creates a new {@code COUNT} function.
	 *
	 * @param columns columns to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction count function} for {@code columns}.
	 */
	public static SimpleFunction count(Column... columns) {

		Assert.notNull(columns, "Columns must not be null!");
		Assert.notEmpty(columns, "Columns must contains at least one column");

		return new SimpleFunction("COUNT", Arrays.asList(columns));
	}

	/**
	 * Creates a new {@code COUNT} function.
	 *
	 * @param columns columns to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction count function} for {@code columns}.
	 */
	public static SimpleFunction count(Collection<? extends Expression> columns) {

		Assert.notNull(columns, "Columns must not be null!");

		return new SimpleFunction("COUNT", new ArrayList<>(columns));
	}

	// Utility constructor.
	private Functions() {
	}
}
