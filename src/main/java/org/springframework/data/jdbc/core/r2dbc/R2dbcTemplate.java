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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.data.jdbc.core.r2dbc.connectionfactory.ConnectionProxy;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

/**
 * @author Mark Paluch
 */
public class R2dbcTemplate extends R2dbcAccessor implements R2dbcOperations {

	/**
	 * Construct a new R2dbcTemplate for bean usage.
	 * <p>
	 * Note: The DataSource has to be set before using the instance.
	 *
	 * @see #setConnectionFactory(ConnectionFactory)
	 */
	public R2dbcTemplate() {}

	/**
	 * Construct a new JdbcTemplate, given a DataSource to obtain connections from.
	 * <p>
	 * Note: This will not trigger initialization of the exception translator.
	 *
	 * @param connectionFactory the R2DBC DataSource to obtain connections from
	 */
	public R2dbcTemplate(ConnectionFactory connectionFactory) {
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	/**
	 * Construct a new {@link R2dbcTemplate}, given a {@link ConnectionFactory} to obtain connections from.
	 * <p>
	 * Note: Depending on the "lazyInit" flag, initialization of the exception translator will be triggered.
	 *
	 * @param connectionFactory the JDBC DataSource to obtain connections from
	 * @param lazyInit whether to lazily initialize the SQLExceptionTranslator
	 */
	public R2dbcTemplate(ConnectionFactory connectionFactory, boolean lazyInit) {
		setConnectionFactory(connectionFactory);
		setLazyInit(lazyInit);
		afterPropertiesSet();
	}

	// -------------------------------------------------------------------------
	// Methods dealing with a plain io.r2dbc.spi.Connection
	// -------------------------------------------------------------------------

	@Override
	public <T> Flux<T> execute(ConnectionCallback<T> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<Connection> connectionMono = Mono.from(obtainConnectionFactory().create());
		// Create close-suppressing Connection proxy, also preparing returned Statements.
		Mono<Connection> conToUse = connectionMono;

		return conToUse.<T> flatMapMany(connection -> {

			AtomicBoolean closed = new AtomicBoolean();
			return doInConnection(action, connection).doFinally(it -> {
				if (closed.compareAndSet(false, true)) {
					Mono.from(connection.close()).subscribe(v -> {}, e -> logger.error("Error on close", e));
				}
			}).doOnCancel(() -> {

				if (closed.compareAndSet(false, true)) {
					Mono.from(connection.close()).subscribe(v -> {}, e -> logger.error("Error on close", e));
				}

			});

		}).onErrorMap(SQLException.class, ex -> {

			String sql = getSql(action);
			return translateException("ConnectionCallback", sql, ex);
		});
	}

	private static <T> Flux<T> doInConnection(ConnectionCallback<T> action, Connection it) {

		try {
			return Flux.from(action.doInConnection(it));
		} catch (RuntimeException e) {

			String sql = getSql(action);
			return Flux.error(new UncategorizedSQLException("ConnectionCallback", sql, e) {

			});
		}
	}

	/**
	 * Create a close-suppressing proxy for the given R2DBC Connection. Called by the {@code execute} method.
	 *
	 * @param con the R2DBC Connection to create a proxy for
	 * @return the Connection proxy
	 * @see #execute(ConnectionCallback)
	 */
	protected Connection createConnectionProxy(Connection con) {
		return (Connection) Proxy.newProxyInstance(ConnectionProxy.class.getClassLoader(),
				new Class<?>[] { ConnectionProxy.class }, new CloseSuppressingInvocationHandler(con));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with static SQL (io.r2dbc.spi.Statement)
	// -------------------------------------------------------------------------

	public <T> Flux<T> execute(String sql, StatementCallback<T> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		class ExecuteCallback implements ConnectionCallback<T>, SqlProvider {

			@Override
			public Publisher<T> doInConnection(Connection con) throws DataAccessException {

				Statement statement = con.createStatement(sql);
				return action.doInStatement(statement);
			}

			@Override
			public String getSql() {
				return sql;
			}
		}

		return execute(new ExecuteCallback());
	}

	@Override
	public Mono<Void> execute(String sql) throws DataAccessException {

		return execute(sql, Statement::execute).doOnSubscribe(s -> {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing SQL statement [" + sql + "]");
			}

		}).then();
	}

	@Override
	public <T> Flux<T> query(String sql, ResultExtractor<T> rse) throws DataAccessException {

		Assert.notNull(sql, "SQL must not be null");
		Assert.notNull(rse, "ResultSetExtractor must not be null");

		return execute(sql, it -> {

			return Mono.from(it.execute()).flatMapMany(rse::extractData);

		}).doOnSubscribe(s -> {
			if (logger.isDebugEnabled()) {
				logger.debug("Executing SQL query [" + sql + "]");
			}
		});
	}

	@Override
	public <T> Flux<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(sql, new RowMapperResultExtractor<>(rowMapper));
	}

	@Override
	public <T> Mono<T> queryForObject(String sql, RowMapper<T> rowMapper) throws DataAccessException {
		return query(sql, rowMapper).buffer(2).flatMap(list -> Mono.just(DataAccessUtils.requiredSingleResult(list)))
				.next();
	}

	@Override
	public <T> Mono<T> queryForObject(String sql, Class<T> requiredType) throws DataAccessException {
		return queryForObject(sql, getSingleColumnRowMapper(requiredType));
	}

	@Override
	public Mono<Map<String, Object>> queryForMap(String sql) throws DataAccessException {
		return queryForObject(sql, getColumnMapRowMapper());
	}

	@Override
	public <T> Flux<T> queryForFlux(String sql, Class<T> elementType) throws DataAccessException {
		return query(sql, getSingleColumnRowMapper(elementType));
	}

	@Override
	public Flux<Map<String, Object>> queryForFlux(String sql) throws DataAccessException {
		return query(sql, getColumnMapRowMapper());
	}

	@Override
	public Mono<Integer> update(String sql) throws DataAccessException {

		Assert.notNull(sql, "SQL must not be null");
		if (logger.isDebugEnabled()) {
			logger.debug("Executing SQL update [" + sql + "]");
		}

		class UpdateStatementCallback implements ConnectionCallback<Integer>, SqlProvider {

			@Override
			public Publisher<Integer> doInConnection(Connection con) throws DataAccessException {

				Flux<Integer> result = Flux.from(con.createStatement(sql).execute()).flatMap(Result::getRowsUpdated);
				if (logger.isDebugEnabled()) {
					result = result.doOnNext(rows -> {
						logger.debug("SQL update affected " + rows + " rows");
					});
				}

				return result;
			}

			@Override
			public String getSql() {
				return sql;
			}
		}

		return execute(new UpdateStatementCallback()).next();
	}

	@Override
	public Flux<Integer> batchUpdate(String... sql) throws DataAccessException {

		Assert.notEmpty(sql, "SQL array must not be empty");
		if (logger.isDebugEnabled()) {
			logger.debug("Executing SQL batch update of " + sql.length + " statements");
		}

		class BatchUpdateStatementCallback implements ConnectionCallback<Integer>, SqlProvider {

			@Nullable private String currSql;

			@Override
			public Publisher<Integer> doInConnection(Connection con) throws DataAccessException {

				Batch batch = con.createBatch();

				for (String sqlStmt : sql) {
					this.currSql = appendSql(this.currSql, sqlStmt);
					batch = batch.add(sqlStmt);
				}

				return Flux.from(batch.execute()).flatMap(Result::getRowsUpdated);
			}

			@Override
			@Nullable
			public String getSql() {
				return this.currSql;
			}

			private String appendSql(@Nullable String sql, String statement) {
				return (StringUtils.isEmpty(sql) ? statement : sql + "; " + statement);
			}
		}

		return execute(new BatchUpdateStatementCallback());
	}

	@Nullable
	public <T> Flux<T> query(String sql, @Nullable StatementSetter pss, ResultExtractor<T> rse)
			throws DataAccessException {

		logger.debug("Executing Statement SQL query with arguments");

		class QueryStatementCallback implements ConnectionCallback<T>, SqlProvider {

			@Override
			public Publisher<T> doInConnection(Connection con) throws DataAccessException {

				Statement statement = con.createStatement(sql);

				if (pss != null) {
					pss.setValues(statement);
					statement = statement.add();
				}

				return Flux.from(statement.execute()).flatMap(rse::extractData);
			}

			@Override
			@Nullable
			public String getSql() {
				return sql;
			}
		}

		return execute(new QueryStatementCallback());
	}

	@Override
	public <T> Flux<T> query(String sql, ResultExtractor<T> rse, Object... args) throws DataAccessException {
		return query(sql, newArgPreparedStatementSetter(args), rse);
	}

	@Override
	public <T> Flux<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return query(sql, new RowMapperResultExtractor<>(rowMapper), args);
	}

	@Override
	public <T> Mono<T> queryForObject(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
		return query(sql, rowMapper, args).buffer(2).flatMap(list -> Mono.just(DataAccessUtils.requiredSingleResult(list)))
				.next();
	}

	@Override
	public <T> Mono<T> queryForObject(String sql, Class<T> requiredType, Object... args) throws DataAccessException {
		return queryForObject(sql, getSingleColumnRowMapper(requiredType), args);
	}

	@Override
	public Mono<Map<String, Object>> queryForMap(String sql, Object... args) throws DataAccessException {
		return queryForObject(sql, getColumnMapRowMapper(), args);
	}

	@Override
	public <T> Flux<T> queryForFlux(String sql, Class<T> elementType, Object... args) throws DataAccessException {
		return query(sql, getSingleColumnRowMapper(elementType), args);
	}

	@Override
	public Flux<Map<String, Object>> queryForFlux(String sql, Object... args) throws DataAccessException {
		return query(sql, getColumnMapRowMapper(), args);
	}

	protected Mono<Integer> update(String sql, @Nullable StatementSetter pss) throws DataAccessException {

		logger.debug("Executing Statement SQL query with arguments");

		class UpdateStatementCallback implements ConnectionCallback<Integer>, SqlProvider {

			@Override
			public Publisher<Integer> doInConnection(Connection con) throws DataAccessException {

				Statement statement = con.createStatement(sql);

				if (pss != null) {
					pss.setValues(statement);
					statement = statement.add();
				}

				return Flux.from(statement.execute()).flatMap(Result::getRowsUpdated);
			}

			@Override
			@Nullable
			public String getSql() {
				return sql;
			}
		}

		return execute(new UpdateStatementCallback()).single();
	}

	@Override
	public Mono<Integer> update(String sql, Object... args) throws DataAccessException {
		return update(sql, newArgPreparedStatementSetter(args));
	}

	// -------------------------------------------------------------------------
	// Implementation hooks and helper methods
	// -------------------------------------------------------------------------

	/**
	 * Create a new RowMapper for reading columns as key-value pairs.
	 *
	 * @return the RowMapper to use
	 * @see ColumnMapRowMapper
	 */
	protected RowMapper<Map<String, Object>> getColumnMapRowMapper() {
		return new ColumnMapRowMapper();
	}

	/**
	 * Create a new RowMapper for reading result objects from a single column.
	 *
	 * @param requiredType the type that each result object is expected to match
	 * @return the RowMapper to use
	 * @see SingleColumnRowMapper
	 */
	protected <T> RowMapper<T> getSingleColumnRowMapper(Class<T> requiredType) {
		return new SingleColumnRowMapper<>(requiredType);
	}

	/**
	 * Create a new arg-based PreparedStatementSetter using the args passed in.
	 * <p>
	 * By default, we'll create an {@link ArgumentStatementSetter}. This method allows for the creation to be overridden
	 * by subclasses.
	 *
	 * @param args object array with arguments
	 * @return the new PreparedStatementSetter to use
	 */
	protected StatementSetter newArgPreparedStatementSetter(@Nullable Object[] args) {
		return new ArgumentStatementSetter(args);
	}

	/**
	 * Translate the given {@link SQLException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param sql SQL query or update that caused the problem (may be {@code null})
	 * @param ex the offending {@code SQLException}
	 * @return a DataAccessException wrapping the {@code SQLException} (never {@code null})
	 * @see #getExceptionTranslator()
	 */
	protected DataAccessException translateException(String task, @Nullable String sql, SQLException ex) {
		DataAccessException dae = getExceptionTranslator().translate(task, sql, ex);
		return (dae != null ? dae : new UncategorizedSQLException(task, sql, ex));
	}

	/**
	 * Determine SQL from potential provider object.
	 *
	 * @param sqlProvider object that's potentially a SqlProvider
	 * @return the SQL string, or {@code null}
	 * @see SqlProvider
	 */
	@Nullable
	private static String getSql(Object sqlProvider) {
		if (sqlProvider instanceof SqlProvider) {
			return ((SqlProvider) sqlProvider).getSql();
		} else {
			return null;
		}
	}

	/**
	 * Invocation handler that suppresses close calls on R2DBC Connections. Also prepares returned Statement
	 * (Prepared/CallbackStatement) objects.
	 *
	 * @see Connection#close()
	 */
	private class CloseSuppressingInvocationHandler implements InvocationHandler {

		private final Connection target;

		public CloseSuppressingInvocationHandler(Connection target) {
			this.target = target;
		}

		@Override
		@Nullable
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// Invocation on ConnectionProxy interface coming in...

			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			} else if (method.getName().equals("hashCode")) {
				// Use hashCode of PersistenceManager proxy.
				return System.identityHashCode(proxy);
			} else if (method.getName().equals("unwrap")) {
				if (((Class<?>) args[0]).isInstance(proxy)) {
					return proxy;
				}
			} else if (method.getName().equals("isWrapperFor")) {
				if (((Class<?>) args[0]).isInstance(proxy)) {
					return true;
				}
			} else if (method.getName().equals("close")) {
				// Handle close method: suppress, not valid.
				return Mono.error(new UnsupportedOperationException("Close is not supported!"));
			} else if (method.getName().equals("getTargetConnection")) {
				// Handle getTargetConnection method: return underlying Connection.
				return this.target;
			}

			// Invoke method on target Connection.
			try {
				Object retVal = method.invoke(this.target, args);

				return retVal;
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

	private static class UncategorizedSQLException extends UncategorizedDataAccessException {

		/** SQL that led to the problem */
		@Nullable private final String sql;

		/**
		 * Constructor for UncategorizedSQLException.
		 *
		 * @param task name of current task
		 * @param sql the offending SQL statement
		 * @param ex the root cause
		 */
		public UncategorizedSQLException(String task, @Nullable String sql, Exception ex) {
			super(String.format("%s; uncategorized SQLException%s; %s", task, sql != null ? " for SQL [" + sql + "]" : "",
					ex.getMessage()), ex);
			this.sql = sql;
		}

		/**
		 * Return the SQL that led to the problem (if known).
		 */
		@Nullable
		public String getSql() {
			return this.sql;
		}
	}
}
