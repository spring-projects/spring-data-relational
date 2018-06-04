/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.r2dbc;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;

import com.nebhale.r2dbc.spi.Statement;

/**
 * Generic callback interface for code that operates on a R2DBC Statement. Allows to execute any number of operations on
 * a single Statement, for example a single {@code executeUpdate} call or repeated {@code executeUpdate} calls with
 * varying SQL.
 * <p>
 * Used internally by R2dbcTemplate, but also useful for application code.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate#execute(StatementCallback)
 */
@FunctionalInterface
public interface StatementCallback<T> {

	/**
	 * Gets called by {@code R2dbcTemplate.execute} with an active R2DBC Statement. Does not need to care about closing
	 * the Statement or the Connection, or about handling transactions: this will all be handled by Spring's
	 * R2dbcTemplate.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects. Note that there's special support for single step actions: see R2dbcTemplate.queryForObject etc. A thrown
	 * RuntimeException is treated as application exception, it gets propagated to the caller of the template.
	 *
	 * @param stmt active R2DBC Statement
	 * @return a result object.
	 * @throws DataAccessException in case of custom exceptions
	 * @see R2dbcTemplate#queryForObject(String, Class)
	 * @see R2dbcTemplate#queryForRowSet(String)
	 */
	Publisher<T> doInStatement(Statement stmt) throws DataAccessException;

}
