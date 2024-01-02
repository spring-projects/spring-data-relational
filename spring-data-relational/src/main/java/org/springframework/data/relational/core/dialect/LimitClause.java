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
package org.springframework.data.relational.core.dialect;

/**
 * A clause representing Dialect-specific {@code LIMIT}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
public interface LimitClause {

	/**
	 * Returns the {@code LIMIT} clause to limit results.
	 *
	 * @param limit the maximum number of lines returned when the resulting SQL snippet is used.
	 * @return rendered limit clause.
	 * @see #getLimitOffset(long, long)
	 */
	String getLimit(long limit);

	/**
	 * Returns the {@code OFFSET} clause to consume rows at a given offset.
	 *
	 * @param offset the numbers of rows that get skipped when the resulting SQL snippet is used.
	 * @return rendered offset clause.
	 * @see #getLimitOffset(long, long)
	 */
	String getOffset(long offset);

	/**
	 * Returns a combined {@code LIMIT/OFFSET} clause that limits results and starts consumption at the given
	 * {@code offset}.
	 *
	 * @param limit  the maximum number of lines returned when the resulting SQL snippet is used.
	 * @param offset the numbers of rows that get skipped when the resulting SQL snippet is used.
	 * @return rendered limit clause.
	 * @see #getLimit(long)
	 * @see #getOffset(long)
	 */
	String getLimitOffset(long limit, long offset);

	/**
	 * Returns the {@link Position} where to apply the {@link #getOffset(long) clause}.
	 */
	Position getClausePosition();

	/**
	 * Enumeration of where to render the clause within the SQL statement.
	 */
	enum Position {

		/**
		 * Append the clause at the end of the statement.
		 */
		AFTER_ORDER_BY
	}
}
