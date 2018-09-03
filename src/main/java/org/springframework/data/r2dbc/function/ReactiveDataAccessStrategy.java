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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.List;
import java.util.function.BiFunction;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.function.convert.SettableValue;

/**
 * @author Mark Paluch
 */
public interface ReactiveDataAccessStrategy {

	/**
	 * @param typeToRead
	 * @return all field names for a specific type.
	 */
	List<String> getAllFields(Class<?> typeToRead);

	/**
	 * @param object
	 * @return {@link SettableValue} that represent an {@code INSERT} of {@code object}.
	 */
	List<SettableValue> getInsert(Object object);

	/**
	 * Map the {@link Sort} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param typeToRead
	 * @param sort
	 * @return
	 */
	Sort getMappedSort(Class<?> typeToRead, Sort sort);

	// TODO: Broaden T to Mono<T>/Flux<T> for reactive relational data access?
	<T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead);

	/**
	 * @param type
	 * @return the table name for the {@link Class entity type}.
	 */
	String getTableName(Class<?> type);
}
