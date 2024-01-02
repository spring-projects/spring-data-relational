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
package org.springframework.data.relational.core.sql;

import java.util.Collection;

/**
 * Entry point to construct an {@link Update} statement.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see StatementBuilder
 */
public interface UpdateBuilder {

	/**
	 * Configure the {@link Table} to which the update is applied.
	 *
	 * @param table the table to update.
	 * @return {@code this} {@link SelectBuilder}.
	 */
	UpdateAssign table(Table table);

	/**
	 * Interface exposing {@code SET} methods.
	 */
	interface UpdateAssign {

		/**
		 * Apply a {@link Assignment SET assignment}.
		 *
		 * @param assignment a single {@link Assignment column assignment}.
		 * @return {@code this} builder.
		 * @see Assignment
		 */
		UpdateWhere set(Assignment assignment);

		/**
		 * Apply one or more {@link Assignment SET assignments}.
		 *
		 * @param assignments the {@link Assignment column assignments}.
		 * @return {@code this} builder.
		 * @see Assignment
		 */
		UpdateWhere set(Assignment... assignments);

		/**
		 * Apply one or more {@link Assignment SET assignments}.
		 *
		 * @param assignments the {@link Assignment column assignments}.
		 * @return {@code this} builder.
		 * @see Assignment
		 */
		UpdateWhere set(Collection<? extends Assignment> assignments);
	}

	/**
	 * Interface exposing {@code WHERE} methods.
	 */
	interface UpdateWhere extends BuildUpdate {

		/**
		 * Apply a {@code WHERE} clause.
		 *
		 * @param condition the {@code WHERE} condition.
		 * @return {@code this} builder.
		 * @see Where
		 * @see Condition
		 */
		UpdateWhereAndOr where(Condition condition);
	}

	/**
	 * Interface exposing {@code AND}/{@code OR} combinator methods for {@code WHERE} {@link Condition}s.
	 */
	interface UpdateWhereAndOr extends BuildUpdate {

		/**
		 * Combine the previous {@code WHERE} {@link Condition} using {@code AND}.
		 *
		 * @param condition the condition, must not be {@literal null}.
		 * @return {@code this} builder.
		 * @see Condition#and(Condition)
		 */
		UpdateWhereAndOr and(Condition condition);

		/**
		 * Combine the previous {@code WHERE} {@link Condition} using {@code OR}.
		 *
		 * @param condition the condition, must not be {@literal null}.
		 * @return {@code this} builder.
		 * @see Condition#or(Condition)
		 */
		UpdateWhereAndOr or(Condition condition);
	}

	/**
	 * Interface exposing the {@link Update} build method.
	 */
	interface BuildUpdate {

		/**
		 * Build the {@link Update}.
		 *
		 * @return the build and immutable {@link Update} statement.
		 */
		Update build();
	}
}
