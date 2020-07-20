/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.r2dbc;

import io.r2dbc.spi.R2dbcException;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.lang.Nullable;

/**
 * Exception thrown when a {@link io.r2dbc.spi.Result} has been accessed in an invalid fashion. Such exceptions always
 * have a {@link io.r2dbc.spi.R2dbcException} root cause.
 * <p>
 * This typically happens when an invalid {@link org.springframework.data.r2dbc.core.FetchSpec} column index or name has
 * been specified.
 *
 * @author Mark Paluch
 * @see BadSqlGrammarException
 * @deprecated since 1.2, not in use anymore.
 */
@SuppressWarnings("serial")
@Deprecated
public class InvalidResultAccessException extends InvalidDataAccessResourceUsageException {

	private final @Nullable String sql;

	/**
	 * Creates a new {@link InvalidResultAccessException}.
	 *
	 * @param task name of current task.
	 * @param sql the offending SQL statement.
	 * @param ex the root cause.
	 */
	public InvalidResultAccessException(String task, @Nullable String sql, R2dbcException ex) {

		super(task + "; invalid Result access for SQL [" + sql + "]", ex);

		this.sql = sql;
	}

	/**
	 * Creates a new {@link InvalidResultAccessException}.
	 *
	 * @param ex the root cause.
	 */
	public InvalidResultAccessException(R2dbcException ex) {

		super(ex.getMessage(), ex);

		this.sql = null;
	}

	/**
	 * Return the wrapped {@link R2dbcException}.
	 */
	public R2dbcException getR2dbcException() {
		return (R2dbcException) getCause();
	}

	/**
	 * Return the SQL that caused the problem.
	 *
	 * @return the offending SQL, if known.
	 */
	@Nullable
	public String getSql() {
		return this.sql;
	}
}
