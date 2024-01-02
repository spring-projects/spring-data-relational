/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import org.springframework.data.domain.Sort;

/**
 * Represents how the {@link Sort.NullHandling} option of an {@code ORDER BY} sort expression is to be evaluated.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
public interface OrderByNullPrecedence {
	/**
	 * An {@link OrderByNullPrecedence} that can be used for databases conforming to the SQL standard which uses
	 * {@code NULLS FIRST} and {@code NULLS LAST} in {@code ORDER BY} sort expressions to make null values appear before
	 * or after non-null values in the result set.
	 */
	OrderByNullPrecedence SQL_STANDARD = new SqlStandardOrderByNullPrecedence();

	/**
	 * An {@link OrderByNullPrecedence} that can be used for databases that do not support the SQL standard usage of
	 * {@code NULLS FIRST} and {@code NULLS LAST} in {@code ORDER BY} sort expressions to control where null values appear
	 * respective to non-null values in the result set.
	 */
	OrderByNullPrecedence NONE = nullHandling -> "";

	/**
	 * Converts a {@link Sort.NullHandling} option to the appropriate SQL text to be included an {@code ORDER BY} sort
	 * expression.
	 */
	String evaluate(Sort.NullHandling nullHandling);

	/**
	 * An {@link OrderByNullPrecedence} implementation for databases conforming to the SQL standard which uses
	 * {@code NULLS FIRST} and {@code NULLS LAST} in {@code ORDER BY} sort expressions to make null values appear before
	 * or after non-null values in the result set.
	 *
	 * @author Chirag Tailor
	 */
	class SqlStandardOrderByNullPrecedence implements OrderByNullPrecedence {

		private static final String NULLS_FIRST = "NULLS FIRST";
		private static final String NULLS_LAST = "NULLS LAST";
		private static final String UNSPECIFIED = "";

		@Override
		public String evaluate(Sort.NullHandling nullHandling) {

			switch (nullHandling) {
				case NULLS_FIRST: return NULLS_FIRST;
				case NULLS_LAST: return NULLS_LAST;
				case NATIVE: return UNSPECIFIED;
				default:
					throw new UnsupportedOperationException("Sort.NullHandling " + nullHandling + " not supported");
			}
		}
	}
}
