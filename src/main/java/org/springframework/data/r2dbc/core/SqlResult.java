/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.function.BiFunction;

/**
 * Mappable {@link FetchSpec} that accepts a {@link BiFunction mapping function} to map SQL {@link Row}s.
 *
 * @author Mark Paluch
 */
interface SqlResult<T> extends FetchSpec<T> {

	/**
	 * Apply a {@link BiFunction mapping function} to the result that emits {@link Row}s.
	 *
	 * @param mappingFunction must not be {@literal null}.
	 * @param <R> the value type of the {@code SqlResult}.
	 * @return a new {@link SqlResult} with {@link BiFunction mapping function} applied.
	 */
	<R> SqlResult<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);
}
