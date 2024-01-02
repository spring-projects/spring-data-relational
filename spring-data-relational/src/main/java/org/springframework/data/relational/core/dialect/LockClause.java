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
package org.springframework.data.relational.core.dialect;

import org.springframework.data.relational.core.sql.LockOptions;

/**
 * A clause representing Dialect-specific {@code LOCK}.
 *
 * @author Myeonghyeon Lee
 * @since 2.0
 */
public interface LockClause {

	/**
	 * Returns the {@code LOCK} clause to lock results.
	 *
	 * @param lockOptions contains the lock mode to apply.
	 * @return rendered lock clause.
	 */
	String getLock(LockOptions lockOptions);

	/**
	 * Returns the {@link Position} where to apply the {@link #getLock(LockOptions) clause}.
	 */
	Position getClausePosition();

	/**
	 * Enumeration of where to render the clause within the SQL statement.
	 */
	enum Position {

		/**
		 * Append the clause after from table.
		 */
		AFTER_FROM_TABLE,

		/**
		 * Append the clause at the end of the statement.
		 */
		AFTER_ORDER_BY
	}
}
