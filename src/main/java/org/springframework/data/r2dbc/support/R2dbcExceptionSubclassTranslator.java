/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;

import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.BadSqlGrammarException;
import org.springframework.lang.Nullable;

/**
 * {@link R2dbcExceptionTranslator} implementation which analyzes the specific {@link R2dbcException} subclass thrown by
 * the R2DBC driver.
 * <p>
 * Falls back to a standard {@link SqlStateR2dbcExceptionTranslator}.
 *
 * @author Mark Paluch
 * @deprecated since 1.2. Use Spring R2DBC's
 *             {@link org.springframework.r2dbc.connection.ConnectionFactoryUtils#convertR2dbcException(String, String, R2dbcException)}
 *             instead.
 */
@Deprecated
public class R2dbcExceptionSubclassTranslator extends AbstractFallbackR2dbcExceptionTranslator {

	public R2dbcExceptionSubclassTranslator() {
		setFallbackTranslator(new SqlStateR2dbcExceptionTranslator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.support.AbstractFallbackR2dbcExceptionTranslator#doTranslate(java.lang.String, java.lang.String, io.r2dbc.spi.R2dbcException)
	 */
	@Override
	@Nullable
	protected DataAccessException doTranslate(String task, @Nullable String sql, R2dbcException ex) {

		if (ex instanceof R2dbcTransientException) {
			if (ex instanceof R2dbcTransientResourceException) {
				return new TransientDataAccessResourceException(buildMessage(task, sql, ex), ex);
			}
			if (ex instanceof R2dbcRollbackException) {
				return new ConcurrencyFailureException(buildMessage(task, sql, ex), ex);
			}
			if (ex instanceof R2dbcTimeoutException) {
				return new QueryTimeoutException(buildMessage(task, sql, ex), ex);
			}
		}

		if (ex instanceof R2dbcNonTransientException) {
			if (ex instanceof R2dbcNonTransientResourceException) {
				return new DataAccessResourceFailureException(buildMessage(task, sql, ex), ex);
			}
			if (ex instanceof R2dbcDataIntegrityViolationException) {
				return new DataIntegrityViolationException(buildMessage(task, sql, ex), ex);
			}
			if (ex instanceof R2dbcPermissionDeniedException) {
				return new PermissionDeniedDataAccessException(buildMessage(task, sql, ex), ex);
			}
			if (ex instanceof R2dbcBadGrammarException) {
				return new BadSqlGrammarException(task, (sql != null ? sql : ""), ex);
			}
		}

		// Fallback to Spring's own R2DBC state translation...
		return null;
	}
}
