/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc;

import io.r2dbc.spi.R2dbcException;

import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.lang.Nullable;

/**
 * Exception thrown when we can't classify a {@link R2dbcException} into one of our generic data access exceptions.
 *
 * @author Mark Paluch
 */
public class UncategorizedR2dbcException extends UncategorizedDataAccessException {

	/**
	 * SQL that led to the problem
	 */
	private final @Nullable String sql;

	/**
	 * Creates a new {@link UncategorizedR2dbcException}.
	 *
	 * @param task name of current task
	 * @param sql the offending SQL statement
	 * @param ex the root cause
	 */
	public UncategorizedR2dbcException(String task, @Nullable String sql, R2dbcException ex) {

		super(String.format("%s; uncategorized R2dbcException%s; %s", task, sql != null ? " for SQL [" + sql + "]" : "",
				ex.getMessage()), ex);
		this.sql = sql;
	}

	/**
	 * Returns the original {@link R2dbcException}.
	 *
	 * @return the original {@link R2dbcException}.
	 */
	public R2dbcException getR2dbcException() {
		return (R2dbcException) getCause();
	}

	/**
	 * Return the SQL that led to the problem (if known).
	 */
	@Nullable
	public String getSql() {
		return this.sql;
	}
}
