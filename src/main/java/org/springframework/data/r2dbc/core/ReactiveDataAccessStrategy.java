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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.List;
import java.util.function.BiFunction;

import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;

/**
 * Data access strategy that generalizes convenience operations using mapped entities. Typically used internally by
 * {@link DatabaseClient} and repository support. SQL creation is limited to single-table operations and single-column
 * primary keys.
 *
 * @author Mark Paluch
 * @see org.springframework.r2dbc.core.PreparedOperation
 */
public interface ReactiveDataAccessStrategy {

	/**
	 * @param entityType
	 * @return all column names for a specific type.
	 */
	List<SqlIdentifier> getAllColumns(Class<?> entityType);

	/**
	 * @param entityType
	 * @return all Id column names for a specific type.
	 */
	List<SqlIdentifier> getIdentifierColumns(Class<?> entityType);

	/**
	 * Returns a {@link OutboundRow} that maps column names to a {@link Parameter} value.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	OutboundRow getOutboundRow(Object object);

	/**
	 * Return a potentially converted {@link SettableValue} for strategies that support type conversion.
	 *
	 * @param value must not be {@literal null}.
	 * @return
	 * @since 1.1
	 * @deprecated since 1.2, use {@link #getBindValue(Parameter)} instead.
	 */
	SettableValue getBindValue(SettableValue value);

	/**
	 * Return a potentially converted {@link SettableValue} for strategies that support type conversion.
	 *
	 * @param value must not be {@literal null}.
	 * @return
	 * @since 1.2
	 */
	Parameter getBindValue(Parameter value);

	/**
	 * Returns a {@link BiFunction row mapping function} to map {@link Row rows} to {@code T}.
	 *
	 * @param typeToRead
	 * @param <T>
	 * @return
	 */
	<T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead);

	/**
	 * @param type
	 * @return the table name for the {@link Class entity type}.
	 */
	SqlIdentifier getTableName(Class<?> type);

	/**
	 * Expand named parameters and return a {@link PreparedOperation} wrapping the given bindings.
	 *
	 * @param query the query to expand.
	 * @param parameterProvider indexed parameter bindings.
	 * @return the {@link PreparedOperation} encapsulating expanded SQL and namedBindings.
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException if a named parameter value cannot be resolved.
	 * @deprecated since 1.2. {@link org.springframework.r2dbc.core.DatabaseClient} encapsulates named parameter handling
	 *             entirely.
	 */
	@Deprecated
	PreparedOperation<?> processNamedParameters(String query, NamedParameterProvider parameterProvider);

	/**
	 * Returns the {@link org.springframework.data.r2dbc.dialect.R2dbcDialect}-specific {@link StatementMapper}.
	 *
	 * @return the {@link org.springframework.data.r2dbc.dialect.R2dbcDialect}-specific {@link StatementMapper}.
	 */
	StatementMapper getStatementMapper();

	/**
	 * Returns the {@link R2dbcConverter}.
	 *
	 * @return the {@link R2dbcConverter}.
	 */
	R2dbcConverter getConverter();

	/**
	 * Render a {@link SqlIdentifier} for SQL usage.
	 *
	 * @param identifier the identifier to be rendered.
	 * @return the SQL representation of the identifier with applied, potentially dialect-specific, processing rules.
	 * @since 1.1
	 * @see SqlIdentifier#toSql(IdentifierProcessing)
	 */
	String toSql(SqlIdentifier identifier);

	/**
	 * Interface to retrieve parameters for named parameter processing.
	 */
	@FunctionalInterface
	interface NamedParameterProvider {

		/**
		 * Returns the {@link SettableValue value} for a parameter identified either by name or by index.
		 *
		 * @param index parameter index according the parameter discovery order.
		 * @param name name of the parameter.
		 * @return the bindable value. Returning a {@literal null} value raises
		 *         {@link org.springframework.dao.InvalidDataAccessApiUsageException} in named parameter processing.
		 */
		@Nullable
		SettableValue getParameter(int index, String name);
	}

}
