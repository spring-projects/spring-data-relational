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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.lang.Nullable;

/**
 * Interface specifying a basic set of R2DBC operations. Implemented by {@link R2dbcTemplate}. Not often used directly,
 * but a useful option to enhance testability, as it can easily be mocked or stubbed.
 * <p>
 * Alternatively, the standard R2DBC infrastructure can be mocked. However, mocking this interface constitutes
 * significantly less work. As an alternative to a mock objects approach to testing data access code, consider the
 * powerful integration testing support provided in the {@code org.springframework.test} package, shipped in
 * {@code spring-test.jar}.
 *
 * @author Mark Paluch
 * @see JdbcTemplate
 */
public interface R2dbcOperations {

	// -------------------------------------------------------------------------
	// Methods dealing with a plain io.r2dbc.spi.Connection
	// -------------------------------------------------------------------------

	/**
	 * Execute a R2DBC data access operation, implemented as callback action working on a R2DBC Connection. This allows
	 * for implementing arbitrary data access operations, within Spring's managed R2DBC environment: that is,
	 * participating in Spring-managed transactions and converting R2DBC Exceptions into Spring's DataAccessException
	 * hierarchy.
	 * <p>
	 * The callback action can return a result object, for example a domain object or a collection of domain objects.
	 *
	 * @param action the callback object that specifies the action
	 * @return a result object returned by the action.
	 * @throws DataAccessException if there is any problem
	 */
	<T> Flux<T> execute(ConnectionCallback<T> action) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with static SQL (io.r2dbc.spi.Statement)
	// -------------------------------------------------------------------------

	/**
	 * Issue a single SQL execute, typically a DDL statement.
	 *
	 * @param sql static SQL to execute
	 * @throws DataAccessException if there is any problem
	 */
	Mono<Void> execute(String sql) throws DataAccessException;

	/**
	 * Execute a query given static SQL, reading the ResultSet with a ResultSetExtractor.
	 * <p>
	 * Uses a R2DBC Statement.
	 *
	 * @param sql SQL query to execute
	 * @param rse object that will extract all rows of results
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #query(String, Object[], ResultExtractor)
	 */
	<T> Flux<T> query(String sql, ResultExtractor<T> rse) throws DataAccessException;

	/**
	 * Execute a query given static SQL, mapping each row to a Java object via a RowMapper.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@code query} method with {@code null} as argument array.
	 *
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #query(String, Object[], RowMapper)
	 */
	<T> Flux<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Execute a query given static SQL, mapping a single result row to a Java object via a RowMapper.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@link #queryForObject(String, RowMapper, Object...)} method with {@code null} as argument
	 * array.
	 *
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @return the single mapped object (may be {@code null} if the given {@link RowMapper} returned {@code} null)
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForObject(String, Object[], RowMapper)
	 */
	<T> Mono<T> queryForObject(String sql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Execute a query for a result object, given static SQL.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@link #queryForObject(String, Class, Object...)} method with {@code null} as argument array.
	 * <p>
	 * This method is useful for running static SQL with a known outcome. The query is expected to be a single row/single
	 * column query; the returned result will be directly mapped to the corresponding object type.
	 *
	 * @param sql SQL query to execute
	 * @param requiredType the type that the result object is expected to match
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForObject(String, Object[], Class)
	 */
	<T> Mono<T> queryForObject(String sql, Class<T> requiredType) throws DataAccessException;

	/**
	 * Execute a query for a result Map, given static SQL.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@link #queryForMap(String, Object...)} method with {@code null} as argument array.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param sql SQL query to execute
	 * @return the result Map (one entry for each column, using the column name as the key)
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForMap(String, Object[])
	 * @see ColumnMapRowMapper
	 */
	Mono<Map<String, Object>> queryForMap(String sql) throws DataAccessException;

	/**
	 * Execute a query for a result list, given static SQL.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@code queryForFlux} method with {@code null} as argument array.
	 * <p>
	 * The results will be mapped to a List (one entry for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param sql SQL query to execute
	 * @param elementType the required type of element in the result list (for example, {@code Integer.class})
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForFlux(String, Object[], Class)
	 * @see SingleColumnRowMapper
	 */
	<T> Flux<T> queryForFlux(String sql, Class<T> elementType) throws DataAccessException;

	/**
	 * Execute a query for a result list, given static SQL.
	 * <p>
	 * Uses a R2DBC Statement, not a PreparedStatement. If you want to execute a static query with a PreparedStatement,
	 * use the overloaded {@code queryForFlux} method with {@code null} as argument array.
	 * <p>
	 * The results will be mapped to a List (one entry for each row) of Maps (one entry for each column using the column
	 * name as the key). Each element in the list will be of the form returned by this interface's queryForMap() methods.
	 *
	 * @param sql SQL query to execute
	 * @return an List that contains a Map per row
	 * @throws DataAccessException if there is any problem executing the query
	 * @see #queryForFlux(String, Object[])
	 */
	Flux<Map<String, Object>> queryForFlux(String sql) throws DataAccessException;

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement).
	 *
	 * @param sql static SQL to execute
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem.
	 */
	Mono<Integer> update(String sql) throws DataAccessException;

	/**
	 * Issue multiple SQL updates on a single R2DBC Statement using batching.
	 * <p>
	 * Will fall back to separate updates on a single Statement if the R2DBC driver does not support batch updates.
	 *
	 * @param sql defining an array of SQL statements that will be executed.
	 * @return an array of the number of rows affected by each statement
	 * @throws DataAccessException if there is any problem executing the batch
	 */
	Flux<Integer> batchUpdate(String... sql) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, reading the
	 * ResultSet with a ResultSetExtractor.
	 *
	 * @param sql SQL query to execute
	 * @param rse object that will extract results
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return an arbitrary result object, as returned by the ResultSetExtractor
	 * @throws DataAccessException if the query fails
	 */
	<T> Flux<T> query(String sql, ResultExtractor<T> rse, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, mapping each
	 * row to a Java object via a RowMapper.
	 *
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return the result List, containing mapped objects
	 * @throws DataAccessException if the query fails
	 */
	<T> Flux<T> query(String sql, RowMapper<T> rowMapper, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, mapping a
	 * single result row to a Java object via a RowMapper.
	 *
	 * @param sql SQL query to execute
	 * @param rowMapper object that will map one object per row
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return the single mapped object (may be {@code null} if the given {@link RowMapper} returned {@code} null)
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if the query fails
	 */
	<T> Mono<T> queryForObject(String sql, RowMapper<T> rowMapper, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, expecting a
	 * result object.
	 * <p>
	 * The query is expected to be a single row/single column query; the returned result will be directly mapped to the
	 * corresponding object type.
	 *
	 * @param sql SQL query to execute
	 * @param requiredType the type that the result object is expected to match
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return the result object of the required type, or {@code null} in case of SQL NULL
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row, or does not return
	 *           exactly one column in that row
	 * @throws DataAccessException if the query fails
	 * @see #queryForObject(String, Class)
	 */
	@Nullable
	<T> Mono<T> queryForObject(String sql, Class<T> requiredType, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, expecting a
	 * result Map. The queryForMap() methods defined by this interface are appropriate when you don't have a domain model.
	 * Otherwise, consider using one of the queryForObject() methods.
	 * <p>
	 * The query is expected to be a single row query; the result row will be mapped to a Map (one entry for each column,
	 * using the column name as the key).
	 *
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return the result Map (one entry for each column, using the column name as the key)
	 * @throws IncorrectResultSizeDataAccessException if the query does not return exactly one row
	 * @throws DataAccessException if the query fails
	 * @see #queryForMap(String)
	 * @see ColumnMapRowMapper
	 */
	Mono<Map<String, Object>> queryForMap(String sql, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, expecting a
	 * result list.
	 * <p>
	 * The results will be mapped to a List (one entry for each row) of result objects, each of them matching the
	 * specified element type.
	 *
	 * @param sql SQL query to execute
	 * @param elementType the required type of element in the result list (for example, {@code Integer.class})
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return a List of objects that match the specified element type
	 * @throws DataAccessException if the query fails
	 * @see #queryForFlux(String, Class)
	 * @see SingleColumnRowMapper
	 */
	<T> Flux<T> queryForFlux(String sql, Class<T> elementType, @Nullable Object... args) throws DataAccessException;

	/**
	 * Query given SQL to create a prepared statement from SQL and a list of arguments to bind to the query, expecting a
	 * result list.
	 * <p>
	 * The results will be mapped to a List (one entry for each row) of Maps (one entry for each column, using the column
	 * name as the key). Each element in the list will be of the form returned by this interface's queryForMap() methods.
	 *
	 * @param sql SQL query to execute
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return a List that contains a Map per row
	 * @throws DataAccessException if the query fails
	 * @see #queryForFlux(String)
	 */
	Flux<Map<String, Object>> queryForFlux(String sql, @Nullable Object... args) throws DataAccessException;

	/**
	 * Issue a single SQL update operation (such as an insert, update or delete statement) via a prepared statement,
	 * binding the given arguments.
	 *
	 * @param sql SQL containing bind parameters
	 * @param args arguments to bind to the query (leaving it to the PreparedStatement to guess the corresponding SQL
	 *          type); may also contain {@link SqlParameterValue} objects which indicate not only the argument value but
	 *          also the SQL type and optionally the scale
	 * @return the number of rows affected
	 * @throws DataAccessException if there is any problem issuing the update
	 */
	Mono<Integer> update(String sql, @Nullable Object... args) throws DataAccessException;
}
