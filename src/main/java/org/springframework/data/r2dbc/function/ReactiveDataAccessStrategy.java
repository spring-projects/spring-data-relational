/*
 * Copyright 2018-2019 the original author or authors.
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
import io.r2dbc.spi.Statement;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.function.convert.SettableValue;

/**
 * Draft of a data access strategy that generalizes convenience operations using mapped entities. Typically used
 * internally by {@link DatabaseClient} and repository support. SQL creation is limited to single-table operations and
 * single-column primary keys.
 *
 * @author Mark Paluch
 * @see BindableOperation
 */
public interface ReactiveDataAccessStrategy {

	/**
	 * @param typeToRead
	 * @return all field names for a specific type.
	 */
	List<String> getAllColumns(Class<?> typeToRead);

	/**
	 * @param object
	 * @return {@link SettableValue} that represent an {@code INSERT} of {@code object}.
	 */
	List<SettableValue> getValuesToInsert(Object object);

	/**
	 * Returns a {@link Map} that maps column names to a {@link SettableValue} value.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	Map<String, SettableValue> getColumnsToUpdate(Object object);

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

	/**
	 * Returns the configured {@link BindMarkersFactory} to create native parameter placeholder markers.
	 *
	 * @return the configured {@link BindMarkersFactory}.
	 */
	BindMarkersFactory getBindMarkersFactory();

	// -------------------------------------------------------------------------
	// Methods creating SQL operations.
	// Subject to be moved into a SQL creation DSL.
	// -------------------------------------------------------------------------

	/**
	 * Create an {@code INSERT} operation for the given {@code table} to insert {@code columns}.
	 *
	 * @param table the table to insert data to.
	 * @param columns column names that will be bound.
	 * @return the {@link BindableOperation} representing the {@code INSERT} statement.
	 */
	BindableOperation insertAndReturnGeneratedKeys(String table, Set<String> columns);

	/**
	 * Create a {@code SELECT … ORDER BY … LIMIT …} operation for the given {@code table} using {@code columns} to
	 * project.
	 *
	 * @param table the table to insert data to.
	 * @param columns columns to return.
	 * @param sort
	 * @param page
	 * @return
	 */
	String select(String table, Set<String> columns, Sort sort, Pageable page);

	/**
	 * Create a {@code UPDATE … SET … WHERE id = ?} operation for the given {@code table} updating {@code columns} and
	 * {@code idColumn}.
	 *
	 * @param table the table to insert data to.
	 * @param columns columns to update.
	 * @param idColumn name of the primary key.
	 * @return
	 */
	BindIdOperation updateById(String table, Set<String> columns, String idColumn);

	/**
	 * Create a {@code DELETE … WHERE id = ?} operation for the given {@code table} and {@code idColumn}.
	 *
	 * @param table the table to insert data to.
	 * @param idColumn name of the primary key.
	 * @return
	 */
	BindIdOperation deleteById(String table, String idColumn);

	/**
	 * Create a {@code DELETE … WHERE id IN (?)} operation for the given {@code table} and {@code idColumn}. The actual
	 * {@link BindableOperation#toQuery() query} string depends on {@link BindIdOperation#bindIds(Statement, Iterable)
	 * bound parameters}.
	 *
	 * @param table the table to insert data to.
	 * @param idColumn name of the primary key.
	 * @return
	 */
	BindIdOperation deleteByIdIn(String table, String idColumn);
}
