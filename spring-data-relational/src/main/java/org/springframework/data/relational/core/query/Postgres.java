/*
 * Copyright 2020-2025 the original author or authors.
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

import static org.springframework.data.relational.core.query.Criteria.CriteriaLiteral;

import java.sql.JDBCType;
import java.util.StringJoiner;

import org.jetbrains.annotations.NotNull;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * PostgreSQL-specific {@link Criteria} conditions.
 *
 * @author Mikhail Polivakha
 */
public class Postgres {

	/**
	 * Custom {@link Criteria} condition builder to check if the column of an {@link java.sql.Types#ARRAY ARRAY} sql type
	 * matches specific conditions. Samples of usage is:
	 * <p>
	 * <pre class="code">
	 * // Code below produces the SQL: "my_column" @> ARRAY['A', 'B']
	 * Postgres.whereArray("my_column").contains("A", "B")
	 * </pre>
	 * Code above produces the SQL:
	 * <pre class="code">
	 *   "my_column" @> ARRAY['A', 'B']
	 * </pre>
	 *
	 * @param arrayName the name of an ARRAY column to match against
	 * @return the {@link OngoingArrayCriteria} to chain future condition
	 */
	public static OngoingArrayCriteria array(String arrayName) {
		return new PostgresCriteriaArray(arrayName);
	}

	public static class PostgresCriteriaArray implements OngoingArrayCriteria {

		private final String arrayColumnName;

		public PostgresCriteriaArray(String arrayColumnName) {
			this.arrayColumnName = arrayColumnName;
		}

		@NotNull
		@Override
		public Criteria
		contains(Object... values) {
			Assert.notNull(values, "values array cannot be null");

			return new Criteria(SqlIdentifier.quoted(arrayColumnName), ExtendedComparator.PostgresExtendedContains.INSTANCE, new CriteriaLiteral() {

				@Override
				public String getLiteral() {
					boolean quoted = true;

					if (values.length > 0) {
						quoted = !Number.class.isAssignableFrom(values[0].getClass());
					}

					return toArrayLiteral(quoted, values);
				}
			});
		}

		@SafeVarargs
		public final <T> String toArrayLiteral(boolean quoted, T... values) {
			StringJoiner accumulator = new StringJoiner(",", "ARRAY[", "]");

			for (T value : values) {
				if (value != null) {
					if (quoted) {
						accumulator.add("'" + value + "'");
					} else {
						accumulator.add(value.toString());
					}
				} else {
					accumulator.add(JDBCType.NULL.name());
				}
			}
			return accumulator.toString();
		}
	}
}
