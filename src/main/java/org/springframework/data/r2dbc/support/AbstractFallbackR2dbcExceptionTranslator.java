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
package org.springframework.data.r2dbc.support;

import io.r2dbc.spi.R2dbcException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.dao.DataAccessException;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base class for {@link R2dbcExceptionTranslator} implementations that allow for fallback to some other
 * {@link R2dbcExceptionTranslator}.
 *
 * @author Mark Paluch
 * @deprecated since 1.2. Use Spring R2DBC's
 *             {@link org.springframework.r2dbc.connection.ConnectionFactoryUtils#convertR2dbcException(String, String, R2dbcException)}
 *             instead.
 */
@Deprecated
public abstract class AbstractFallbackR2dbcExceptionTranslator implements R2dbcExceptionTranslator {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	@Nullable private R2dbcExceptionTranslator fallbackTranslator;

	/**
	 * Override the default SQL state fallback translator (typically a {@link R2dbcExceptionTranslator}).
	 */
	public void setFallbackTranslator(@Nullable R2dbcExceptionTranslator fallback) {
		this.fallbackTranslator = fallback;
	}

	/**
	 * Return the fallback exception translator, if any.
	 */
	@Nullable
	public R2dbcExceptionTranslator getFallbackTranslator() {
		return this.fallbackTranslator;
	}

	/**
	 * Pre-checks the arguments, calls {@link #doTranslate}, and invokes the {@link #getFallbackTranslator() fallback
	 * translator} if necessary.
	 */
	@Override
	@NonNull
	public DataAccessException translate(String task, @Nullable String sql, R2dbcException ex) {

		Assert.notNull(ex, "Cannot translate a null R2dbcException");

		DataAccessException dae = doTranslate(task, sql, ex);
		if (dae != null) {
			// Specific exception match found.
			return dae;
		}

		// Looking for a fallback...
		R2dbcExceptionTranslator fallback = getFallbackTranslator();
		if (fallback != null) {
			dae = fallback.translate(task, sql, ex);
			if (dae != null) {
				// Fallback exception match found.
				return dae;
			}
		}

		// We couldn't identify it more precisely.
		return new UncategorizedR2dbcException(task, sql, ex);
	}

	/**
	 * Template method for actually translating the given exception.
	 * <p>
	 * The passed-in arguments will have been pre-checked. Furthermore, this method is allowed to return {@literal null}
	 * to indicate that no exception match has been found and that fallback translation should kick in.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param sql SQL query or update that caused the problem (if known).
	 * @param ex the offending {@link R2dbcException}.
	 * @return the DataAccessException, wrapping the {@link R2dbcException}; or {@literal null} if no exception match
	 *         found.
	 */
	@Nullable
	protected abstract DataAccessException doTranslate(String task, @Nullable String sql, R2dbcException ex);

	/**
	 * Build a message {@code String} for the given {@link R2dbcException}.
	 * <p>
	 * To be called by translator subclasses when creating an instance of a generic
	 * {@link org.springframework.dao.DataAccessException} class.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param sql the SQL statement that caused the problem.
	 * @param ex the offending {@link R2dbcException}.
	 * @return the message {@code String} to use.
	 */
	protected String buildMessage(String task, @Nullable String sql, R2dbcException ex) {

		return task + "; " + //
				(sql != null //
						? "SQL [" + sql + "]; " //
						: "" //
				) + ex.getMessage();
	}
}
