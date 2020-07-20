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

import org.springframework.lang.Nullable;

/**
 * Exception thrown when we can't classify a {@link R2dbcException} into one of our generic data access exceptions.
 *
 * @author Mark Paluch
 * @deprecated since 1.2, use Spring R2DBC's {@link org.springframework.r2dbc.UncategorizedR2dbcException} instead.
 */
@Deprecated
public class UncategorizedR2dbcException extends org.springframework.r2dbc.UncategorizedR2dbcException {

	private static final long serialVersionUID = 361587356435210266L;

	/**
	 * Creates a new {@link UncategorizedR2dbcException}.
	 *
	 * @param task name of current task
	 * @param sql the offending SQL statement
	 * @param ex the root cause
	 */
	public UncategorizedR2dbcException(String task, @Nullable String sql, R2dbcException ex) {
		super(task, sql, ex);
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
		return super.getSql();
	}
}
