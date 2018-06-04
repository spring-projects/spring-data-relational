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

import reactor.core.publisher.Mono;

import com.nebhale.r2dbc.spi.Result;

/**
 * An interface used by {@link R2dbcTemplate} for processing rows of a {@link Row} on a per-row basis. Implementations
 * of this interface perform the actual work of processing each row but don't need to worry about exception handling.
 * <p>
 * In contrast to a {@link ResultExtractor}, a RowCallbackHandler object is typically stateful: It keeps the result
 * state within the object, to be available for later inspection.
 * <p>
 * Consider using a {@link RowMapper} instead if you need to map exactly one result object per row, assembling them into
 * a List.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate
 * @see RowMapper
 * @see ResultExtractor
 */
@FunctionalInterface
public interface RowCallbackHandler {

	/**
	 * Implementations must implement this method to process each row of data in the Result.
	 * <p>
	 * Exactly what the implementation chooses to do is up to it: A trivial implementation might simply count rows, while
	 * another implementation might build an XML document.
	 *
	 * @param rs the ResultSet to process (pre-initialized for the current row)
	 */
	Mono<Void> processRow(Result rs);

}
