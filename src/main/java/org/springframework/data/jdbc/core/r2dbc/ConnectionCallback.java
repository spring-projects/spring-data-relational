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

import io.r2dbc.spi.Connection;

/**
 * Generic callback interface for code that operates on a R2DBC Connection. Allows to execute any number of operations
 * on a single Connection, using any type and number of Statements.
 * <p>
 * This is particularly useful for delegating to existing data access code that expects a {@link Connection} to work on.
 * For newly written code, it is strongly recommended to use R2dbcTemplate's more specific operations, for example a
 * {@code query} or {@code update} variant.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate#execute(ConnectionCallback)
 * @see R2dbcTemplate#query
 * @see R2dbcTemplate#update
 */
@FunctionalInterface
public interface ConnectionCallback<T> {

	/**
	 * Gets called by {@code R2dbcTemplate.execute} with an active R2DBC Connection. Does not need to care about
	 * activating or closing the {@link Connection}, or handling transactions.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * {@link R2dbcTemplate}. Note that there's special support for single step actions: see
	 * {@link R2dbcTemplate#queryForObject} etc. A thrown {@link RuntimeException} is treated as application exception: it
	 * gets propagated to the caller of the template.
	 *
	 * @param con active R2DBC Connection
	 * @return a result object.
	 * @throws DataAccessException in case of custom exceptions
	 * @see R2dbcTemplate#queryForObject(String, Class)
	 * @see R2dbcTemplate#queryForRowSet(String)
	 */
	Publisher<T> doInConnection(Connection con) throws DataAccessException;
}
