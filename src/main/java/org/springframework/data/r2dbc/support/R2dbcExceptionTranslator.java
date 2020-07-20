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

import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;

/**
 * Strategy interface for translating between {@link io.r2dbc.spi.R2dbcException R2dbcExceptions} and Spring's data
 * access strategy-agnostic {@link DataAccessException} hierarchy.
 * <p>
 * Implementations can be generic (for example, using {@link io.r2dbc.spi.R2dbcException#getSqlState() SQLState} codes
 * for R2DBC) or wholly proprietary (for example, using Oracle error codes) for greater precision.
 *
 * @author Mark Paluch
 * @see org.springframework.dao.DataAccessException
 * @see SqlStateR2dbcExceptionTranslator
 * @see SqlErrorCodeR2dbcExceptionTranslator
 * @deprecated since 1.2. Use Spring R2DBC's
 *             {@link org.springframework.r2dbc.connection.ConnectionFactoryUtils#convertR2dbcException(String, String, R2dbcException)}
 *             instead.
 */
@FunctionalInterface
@Deprecated
public interface R2dbcExceptionTranslator {

	/**
	 * Translate the given {@link R2dbcException} into a generic {@link DataAccessException}.
	 * <p>
	 * The returned DataAccessException is supposed to contain the original {@link R2dbcException} as root cause. However,
	 * client code may not generally rely on this due to DataAccessExceptions possibly being caused by other resource APIs
	 * as well. That said, a {@code getRootCause() instanceof R2dbcException} check (and subsequent cast) is considered
	 * reliable when expecting R2DBC-based access to have happened.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param sql SQL query or update that caused the problem (if known).
	 * @param ex the offending {@link R2dbcException}.
	 * @return the DataAccessException wrapping the {@code R2dbcException}, or {@literal null} if no translation could be
	 *         applied (in a custom translator; the default translators always throw an
	 *         {@link org.springframework.data.r2dbc.UncategorizedR2dbcException} in such a case).
	 * @see org.springframework.dao.DataAccessException#getRootCause()
	 */
	@Nullable
	DataAccessException translate(String task, @Nullable String sql, R2dbcException ex);

}
