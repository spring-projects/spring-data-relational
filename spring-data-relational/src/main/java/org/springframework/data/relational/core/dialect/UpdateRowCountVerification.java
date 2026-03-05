/*
 * Copyright 2026-present the original author or authors.
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

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;

/**
 * Defines whether the result of a save/update is considered an error.
 * <p>
 * Database and driver behavior differs: some report <em>affected</em> rows (e.g. MySQL/InnoDB can report 0 for a no-op
 * update), others report <em>matched</em> rows. Use {@link #LENIENT} when the database may legitimately report 0 rows
 * for a successful no-op update; use {@link #STRICT} when you want to detect missing rows or failed updates.
 *
 * @since 4.1
 */
@FunctionalInterface
public interface UpdateRowCountVerification {

	/**
	 * Do not throw when an UPDATE affects 0 rows. Use when the database or driver may report 0 for a no-op update (e.g.
	 * MySQL/InnoDB with affected-rows semantics, Vitess).
	 */
	UpdateRowCountVerification LENIENT = (rowsModified) -> {};

	/**
	 * Throw {@link org.springframework.dao.IncorrectUpdateSemanticsDataAccessException} when an UPDATE affects 0 rows.
	 * Use to detect missing rows, RLS-blocked updates, or stale identifiers.
	 */
	UpdateRowCountVerification STRICT = (rowsModified) -> {
		throw new IncorrectUpdateSemanticsDataAccessException("No rows were updated");
	};

	/**
	 * @param rowsModified flag to indicate whether the update affected any rows.
	 * @throws IncorrectUpdateSemanticsDataAccessException in case the update did not affect any rows and this is
	 *           considered a failed operation.
	 */
	void rowsModified(boolean rowsModified);

	/**
	 * @param nrRows number of rows affected by the update.
	 * @throws IncorrectUpdateSemanticsDataAccessException in case the update did not affect any rows and this is
	 *           considered a failed operation.
	 */
	default void rowsModified(long nrRows) {
		rowsModified(nrRows > 0);
	}

}
