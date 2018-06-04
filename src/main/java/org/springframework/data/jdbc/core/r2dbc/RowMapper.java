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

import java.util.function.BiFunction;

import org.springframework.lang.Nullable;

import com.nebhale.r2dbc.spi.Row;
import com.nebhale.r2dbc.spi.RowMetadata;

/**
 * An interface used by {@link R2dbcTemplate} for mapping rows of a {@link Result} on a per-row basis. Implementations
 * of this interface perform the actual work of mapping each row to a result object.
 * <p>
 * Typically used either for {@link R2dbcTemplate}'s query methods or for out parameters of stored procedures. RowMapper
 * objects are typically stateless and thus reusable; they are an ideal choice for implementing row-mapping logic in a
 * single place.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate
 * @see RowCallbackHandler
 * @see ResultExtractor
 */
@FunctionalInterface
public interface RowMapper<T> extends BiFunction<Row, RowMetadata, T> {

	@Override
	default T apply(Row row, RowMetadata rowMetadata) {
		return mapRow(row);
	}

	/**
	 * Implementations must implement this method to map each row of data in the {@link com.nebhale.r2dbc.spi.Result}.
	 *
	 * @param row the {@link Row} to map.
	 * @return the result object for the current row (may be {@code null})
	 */
	@Nullable
	T mapRow(Row rs);
}
