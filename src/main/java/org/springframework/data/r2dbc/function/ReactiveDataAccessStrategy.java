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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.List;
import java.util.function.BiFunction;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.domain.BindableOperation;
import org.springframework.data.r2dbc.domain.Bindings;
import org.springframework.data.r2dbc.domain.OutboundRow;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.r2dbc.function.convert.R2dbcConverter;
import org.springframework.data.r2dbc.function.query.BoundCondition;
import org.springframework.data.r2dbc.function.query.Criteria;
import org.springframework.data.relational.core.sql.Table;

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
	 * Returns a {@link OutboundRow} that maps column names to a {@link SettableValue} value.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	OutboundRow getOutboundRow(Object object);

	/**
	 * Map the {@link Sort} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param sort must not be {@literal null}.
	 * @param typeToRead must not be {@literal null}.
	 * @return
	 */
	Sort getMappedSort(Sort sort, Class<?> typeToRead);

	/**
	 * Map the {@link Criteria} object to apply value mapping and return a {@link BoundCondition} with {@link Bindings}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @return
	 */
	BoundCondition getMappedCriteria(Criteria criteria, Table table);

	/**
	 * Map the {@link Criteria} object to apply value and field name mapping and return a {@link BoundCondition} with
	 * {@link Bindings}.
	 *
	 * @param criteria must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @return
	 */
	BoundCondition getMappedCriteria(Criteria criteria, Table table, Class<?> typeToRead);

	// TODO: Broaden T to Mono<T>/Flux<T> for reactive relational data access?
	<T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead);

	/**
	 * @param type
	 * @return the table name for the {@link Class entity type}.
	 */
	String getTableName(Class<?> type);

	/**
	 * Returns the {@link Dialect}-specific {@link StatementFactory}.
	 *
	 * @return the {@link Dialect}-specific {@link StatementFactory}.
	 */
	StatementFactory getStatements();

	/**
	 * Returns the configured {@link BindMarkersFactory} to create native parameter placeholder markers.
	 *
	 * @return the configured {@link BindMarkersFactory}.
	 */
	BindMarkersFactory getBindMarkersFactory();

	/**
	 * Returns the {@link R2dbcConverter}.
	 *
	 * @return the {@link R2dbcConverter}.
	 */
	R2dbcConverter getConverter();

}
