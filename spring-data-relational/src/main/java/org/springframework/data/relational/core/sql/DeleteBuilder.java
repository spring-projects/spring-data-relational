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

/**
 * Entry point to construct a {@link Delete} statement.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see StatementBuilder
 */
public interface DeleteBuilder {

	/**
	 * Declare a {@link Table} for {@code DELETE FROM}.
	 *
	 * @param table the table to {@code DELETE FROM} must not be {@literal null}.
	 * @return {@code this} builder.
	 * @see From
	 * @see SQL#table(String)
	 */
	DeleteWhere from(Table table);

	/**
	 * Interface exposing {@code WHERE} methods.
	 */
	interface DeleteWhere extends BuildDelete {

		/**
		 * Apply a {@code WHERE} clause.
		 *
		 * @param condition the {@code WHERE} condition.
		 * @return {@code this} builder.
		 * @see Where
		 * @see Condition
		 */
		DeleteWhereAndOr where(Condition condition);
	}

	/**
	 * Interface exposing {@code AND}/{@code OR} combinator methods for {@code WHERE} {@link Condition}s.
	 */
	interface DeleteWhereAndOr extends BuildDelete {

		/**
		 * Combine the previous {@code WHERE} {@link Condition} using {@code AND}.
		 *
		 * @param condition the condition, must not be {@literal null}.
		 * @return {@code this} builder.
		 * @see Condition#and(Condition)
		 */
		DeleteWhereAndOr and(Condition condition);

		/**
		 * Combine the previous {@code WHERE} {@link Condition} using {@code OR}.
		 *
		 * @param condition the condition, must not be {@literal null}.
		 * @return {@code this} builder.
		 * @see Condition#or(Condition)
		 */
		DeleteWhereAndOr or(Condition condition);
	}

	/**
	 * Interface exposing the {@link Delete} build method.
	 */
	interface BuildDelete {

		/**
		 * Build the {@link Delete} statement and verify basic relationship constraints such as all referenced columns have
		 * a {@code FROM} table import.
		 *
		 * @return the build and immutable {@link Delete} statement.
		 */
		Delete build();
	}
}
