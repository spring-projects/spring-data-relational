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
package org.springframework.data.jdbc.core.function;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.data.jdbc.core.function.connectionfactory.ConnectionProxy;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.support.SQLExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
class DefaultDatabaseClient implements DatabaseClient {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private final ConnectionFactory connector;

	private final SQLExceptionTranslator exceptionTranslator;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final DefaultDatabaseClientBuilder builder;

	public DefaultDatabaseClient(ConnectionFactory connector, SQLExceptionTranslator exceptionTranslator,
			ReactiveDataAccessStrategy dataAccessStrategy, DefaultDatabaseClientBuilder builder) {

		this.connector = connector;
		this.exceptionTranslator = exceptionTranslator;
		this.dataAccessStrategy = dataAccessStrategy;
		this.builder = builder;
	}

	@Override
	public Builder mutate() {
		return builder;
	}

	@Override
	public SqlSpec execute() {
		return new DefaultSqlSpec();
	}

	@Override
	public SelectFromSpec select() {
		throw new UnsupportedOperationException("Implement me");
	}

	@Override
	public InsertIntoSpec insert() {
		return new DefaultInsertIntoSpec();
	}

	/**
	 * Execute a callback {@link Function} within a {@link Connection} scope. The function is responsible for creating a
	 * {@link Flux}. The connection is released after the {@link Flux} terminates (or the subscription is cancelled).
	 * Connection resources must not be passed outside of the {@link Function} closure, otherwise resources may get
	 * defunct.
	 *
	 * @param action must not be {@literal null}.
	 * @return the resulting {@link Flux}.
	 * @throws DataAccessException
	 */
	public <T> Flux<T> executeMany(Function<Connection, Flux<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<Connection> connectionMono = getConnection();
		// Create close-suppressing Connection proxy, also preparing returned Statements.

		return Flux.usingWhen(connectionMono, it -> {

			Connection connectionToUse = createConnectionProxy(it);

			return doInConnectionMany(connectionToUse, action);
		}, this::closeConnection, this::closeConnection, this::closeConnection) //
				.onErrorMap(SQLException.class, ex -> translateException("executeMany", getSql(action), ex));
	}

	/**
	 * Execute a callback {@link Function} within a {@link Connection} scope. The function is responsible for creating a
	 * {@link Mono}. The connection is released after the {@link Mono} terminates (or the subscription is cancelled).
	 * Connection resources must not be passed outside of the {@link Function} closure, otherwise resources may get
	 * defunct.
	 *
	 * @param action must not be {@literal null}.
	 * @return the resulting {@link Mono}.
	 * @throws DataAccessException
	 */
	public <T> Mono<T> execute(Function<Connection, Mono<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<Connection> connectionMono = getConnection();
		// Create close-suppressing Connection proxy, also preparing returned Statements.

		return Mono.usingWhen(connectionMono, it -> {

			Connection connectionToUse = createConnectionProxy(it);

			return doInConnection(connectionToUse, action);
		}, this::closeConnection, this::closeConnection, this::closeConnection) //
				.onErrorMap(SQLException.class, ex -> translateException("execute", getSql(action), ex));
	}

	/**
	 * Obtain a {@link Connection}.
	 *
	 * @return
	 */
	protected Mono<Connection> getConnection() {
		return Mono.from(obtainConnectionFactory().create());
	}

	/**
	 * Release the {@link Connection}.
	 *
	 * @param connection
	 * @return
	 */
	protected Publisher<Void> closeConnection(Connection connection) {
		return connection.close();
	}

	/**
	 * Obtain the {@link ConnectionFactory} for actual use.
	 *
	 * @return the ConnectionFactory (never {@code null})
	 * @throws IllegalStateException in case of no DataSource set
	 */
	protected ConnectionFactory obtainConnectionFactory() {
		return connector;
	}

	/**
	 * Create a close-suppressing proxy for the given R2DBC Connection. Called by the {@code execute} method.
	 *
	 * @param con the R2DBC Connection to create a proxy for
	 * @return the Connection proxy
	 */
	protected Connection createConnectionProxy(Connection con) {
		return (Connection) Proxy.newProxyInstance(ConnectionProxy.class.getClassLoader(),
				new Class<?>[] { ConnectionProxy.class }, new CloseSuppressingInvocationHandler(con));
	}

	/**
	 * Translate the given {@link SQLException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param sql SQL query or update that caused the problem (may be {@code null})
	 * @param ex the offending {@code SQLException}
	 * @return a DataAccessException wrapping the {@code SQLException} (never {@code null})
	 */
	protected DataAccessException translateException(String task, @Nullable String sql, SQLException ex) {

		DataAccessException dae = exceptionTranslator.translate(task, sql, ex);
		return (dae != null ? dae : new UncategorizedSQLException(task, sql, ex));
	}

	private static void doBind(Statement statement, Map<String, Optional<Object>> byName,
			Map<Integer, Optional<Object>> byIndex) {

		byIndex.forEach((i, o) -> {

			if (o.isPresent()) {
				o.ifPresent(v -> statement.bind(i, v));
			} else {
				statement.bindNull(i, 0); // TODO: What is type?
			}
		});

		byName.forEach((name, o) -> {

			if (o.isPresent()) {
				o.ifPresent(v -> statement.bind(name, v));
			} else {
				statement.bindNull(name, 0); // TODO: What is type?
			}
		});

	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.DatabaseClient.SqlSpec} implementation.
	 */
	private class DefaultSqlSpec implements SqlSpec {

		@Override
		public GenericExecuteSpec sql(String sql) {

			Assert.hasText(sql, "SQL must not be null or empty!");
			return sql(() -> sql);
		}

		@Override
		public GenericExecuteSpec sql(Supplier<String> sqlSupplier) {

			Assert.notNull(sqlSupplier, "SQL Supplier must not be null!");

			return new DefaultGenericExecuteSpec(sqlSupplier);
		}
	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.DatabaseClient.GenericExecuteSpec} implementation.
	 */
	@RequiredArgsConstructor
	private class GenericExecuteSpecSupport {

		final Map<Integer, Optional<Object>> byIndex;
		final Map<String, Optional<Object>> byName;
		final Supplier<String> sqlSupplier;

		GenericExecuteSpecSupport(Supplier<String> sqlSupplier) {

			this.byIndex = Collections.emptyMap();
			this.byName = Collections.emptyMap();
			this.sqlSupplier = sqlSupplier;
		}

		protected String getSql() {

			String sql = sqlSupplier.get();
			Assert.state(sql != null, "SQL supplier returned null!");
			return sql;
		}

		protected <T> SqlResult<T> exchange(String sql, BiFunction<Row, RowMetadata, T> mappingFunction) {

			Function<Connection, Statement> executeFunction = it -> {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing SQL statement [" + sql + "]");
				}

				Statement statement = it.createStatement(sql);
				doBind(statement, byName, byIndex);

				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(executeFunction.apply(it).execute());

			return new DefaultSqlResultFunctions<>(sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}

		public GenericExecuteSpecSupport bind(int index, Object value) {

			Map<Integer, Optional<Object>> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, Optional.of(value));

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bindNull(int index) {

			Map<Integer, Optional<Object>> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, Optional.empty());

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bind(String name, Object value) {

			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, Optional<Object>> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, Optional.of(value));

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bindNull(String name) {

			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, Optional<Object>> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, Optional.empty());

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		protected GenericExecuteSpecSupport createInstance(Map<Integer, Optional<Object>> byIndex,
				Map<String, Optional<Object>> byName, Supplier<String> sqlSupplier) {
			return new GenericExecuteSpecSupport(byIndex, byName, sqlSupplier);
		}

		public GenericExecuteSpecSupport bind(Object bean) {

			Assert.notNull(bean, "Bean must not be null!");

			throw new UnsupportedOperationException("Implement me!");
		}
	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.DatabaseClient.GenericExecuteSpec} implementation.
	 */
	private class DefaultGenericExecuteSpec extends GenericExecuteSpecSupport implements GenericExecuteSpec {

		DefaultGenericExecuteSpec(Map<Integer, Optional<Object>> byIndex, Map<String, Optional<Object>> byName,
				Supplier<String> sqlSupplier) {
			super(byIndex, byName, sqlSupplier);
		}

		DefaultGenericExecuteSpec(Supplier<String> sqlSupplier) {
			super(sqlSupplier);
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return new DefaultTypedGenericExecuteSpec<>(this.byIndex, this.byName, this.sqlSupplier, resultType);
		}

		@Override
		public FetchSpec<Map<String, Object>> fetch() {
			return exchange(getSql(), ColumnMapRowMapper.INSTANCE);
		}

		@Override
		public Mono<SqlResult<Map<String, Object>>> exchange() {
			return Mono.just(exchange(getSql(), ColumnMapRowMapper.INSTANCE));
		}

		@Override
		public DefaultGenericExecuteSpec bind(int index, Object value) {
			return (DefaultGenericExecuteSpec) super.bind(index, value);
		}

		@Override
		public DefaultGenericExecuteSpec bindNull(int index) {
			return (DefaultGenericExecuteSpec) super.bindNull(index);
		}

		@Override
		public DefaultGenericExecuteSpec bind(String name, Object value) {
			return (DefaultGenericExecuteSpec) super.bind(name, value);
		}

		@Override
		public DefaultGenericExecuteSpec bindNull(String name) {
			return (DefaultGenericExecuteSpec) super.bindNull(name);
		}

		@Override
		public DefaultGenericExecuteSpec bind(Object bean) {
			return (DefaultGenericExecuteSpec) super.bind(bean);
		}

		@Override
		protected GenericExecuteSpecSupport createInstance(Map<Integer, Optional<Object>> byIndex,
				Map<String, Optional<Object>> byName, Supplier<String> sqlSupplier) {
			return new DefaultGenericExecuteSpec(byIndex, byName, sqlSupplier);
		}
	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.DatabaseClient.GenericExecuteSpec} implementation.
	 */
	@SuppressWarnings("unchecked")
	private class DefaultTypedGenericExecuteSpec<T> extends GenericExecuteSpecSupport implements TypedExecuteSpec<T> {

		private final Class<T> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedGenericExecuteSpec(Map<Integer, Optional<Object>> byIndex, Map<String, Optional<Object>> byName,
				Supplier<String> sqlSupplier, Class<T> typeToRead) {

			super(byIndex, byName, sqlSupplier);

			this.typeToRead = typeToRead;
			this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return new DefaultTypedGenericExecuteSpec<>(this.byIndex, this.byName, this.sqlSupplier, resultType);
		}

		@Override
		public FetchSpec<T> fetch() {
			return exchange(getSql(), mappingFunction);
		}

		@Override
		public Mono<SqlResult<T>> exchange() {
			return Mono.just(exchange(getSql(), mappingFunction));
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bind(int index, Object value) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bind(index, value);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bindNull(int index) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bindNull(index);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bind(String name, Object value) {
			return (DefaultTypedGenericExecuteSpec) super.bind(name, value);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bindNull(String name) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bindNull(name);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bind(Object bean) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bind(bean);
		}

		@Override
		protected DefaultTypedGenericExecuteSpec<T> createInstance(Map<Integer, Optional<Object>> byIndex,
				Map<String, Optional<Object>> byName, Supplier<String> sqlSupplier) {
			return new DefaultTypedGenericExecuteSpec<>(byIndex, byName, sqlSupplier, typeToRead);
		}
	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.DatabaseClient.InsertIntoSpec} implementation.
	 */
	class DefaultInsertIntoSpec implements InsertIntoSpec {

		@Override
		public GenericInsertSpec into(String table) {
			return new DefaultGenericInsertSpec(table, Collections.emptyMap());
		}

		@Override
		public <T> TypedInsertSpec<T> into(Class<T> table) {
			return new DefaultTypedInsertSpec<>(table);
		}
	}

	/**
	 * Default implementation of {@link org.springframework.data.jdbc.core.function.DatabaseClient.GenericInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultGenericInsertSpec implements GenericInsertSpec {

		private final String table;
		private final Map<String, Optional<Object>> byName;

		@Override
		public GenericInsertSpec value(String field, Object value) {

			Assert.notNull(field, "Field must not be null!");

			Map<String, Optional<Object>> byName = new LinkedHashMap<>(this.byName);
			byName.put(field, Optional.of(value));

			return new DefaultGenericInsertSpec(this.table, byName);
		}

		@Override
		public GenericInsertSpec nullValue(String field) {

			Assert.notNull(field, "Field must not be null!");

			Map<String, Optional<Object>> byName = new LinkedHashMap<>(this.byName);
			byName.put(field, Optional.empty());

			return new DefaultGenericInsertSpec(this.table, byName);
		}

		@Override
		public Mono<Void> then() {
			return exchange((row, md) -> row).all().then();
		}

		@Override
		public Mono<SqlResult<Map<String, Object>>> exchange() {
			return Mono.just(exchange(ColumnMapRowMapper.INSTANCE));
		}

		private <T> SqlResult<T> exchange(BiFunction<Row, RowMetadata, T> mappingFunction) {

			if (byName.isEmpty()) {
				throw new IllegalStateException("Insert fields is empty!");
			}

			StringBuilder builder = new StringBuilder();
			String fieldNames = byName.keySet().stream().collect(Collectors.joining(","));
			String placeholders = IntStream.range(0, byName.size()).mapToObj(i -> "$" + (i + 1))
					.collect(Collectors.joining(","));

			builder.append("INSERT INTO ").append(table).append(" (").append(fieldNames).append(") ").append(" VALUES(")
					.append(placeholders).append(")");

			String sql = builder.toString();
			Function<Connection, Statement> insertFunction = it -> {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing SQL statement [" + sql + "]");
				}
				Statement statement = it.createStatement(sql);
				doBind(statement);
				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = it -> Flux
					.from(insertFunction.apply(it).executeReturningGeneratedKeys());

			return new DefaultSqlResultFunctions<>(sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}

		/**
		 * PostgreSQL-specific bind.
		 *
		 * @param statement
		 */
		private void doBind(Statement statement) {

			AtomicInteger index = new AtomicInteger();

			for (Optional<Object> o : byName.values()) {

				if (o.isPresent()) {
					o.ifPresent(v -> statement.bind(index.getAndIncrement(), v));
				} else {
					statement.bindNull("$" + (index.getAndIncrement() + 1), 0); // TODO: What is type?
				}
			}
		}
	}

	/**
	 * Default implementation of {@link org.springframework.data.jdbc.core.function.DatabaseClient.TypedInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultTypedInsertSpec<T> implements TypedInsertSpec<T>, InsertSpec {

		private final Class<?> typeToInsert;
		private final String table;
		private final Publisher<T> objectToInsert;

		DefaultTypedInsertSpec(Class<?> typeToInsert) {

			this.typeToInsert = typeToInsert;
			this.table = dataAccessStrategy.getTableName(typeToInsert);
			this.objectToInsert = Mono.empty();
		}

		@Override
		public TypedInsertSpec<T> table(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty!");

			return new DefaultTypedInsertSpec<>(typeToInsert, tableName, objectToInsert);
		}

		@Override
		public InsertSpec using(T objectToInsert) {

			Assert.notNull(objectToInsert, "Object to insert must not be null!");

			return new DefaultTypedInsertSpec<>(typeToInsert, table, Mono.just(objectToInsert));
		}

		@Override
		public InsertSpec using(Publisher<T> objectToInsert) {

			Assert.notNull(objectToInsert, "Publisher to insert must not be null!");

			return new DefaultTypedInsertSpec<>(typeToInsert, table, objectToInsert);
		}

		@Override
		public Mono<Void> then() {
			return Mono.from(objectToInsert).map(toInsert -> exchange(toInsert, (row, md) -> row).all()).then();
		}

		@Override
		public Mono<SqlResult<Map<String, Object>>> exchange() {
			return Mono.from(objectToInsert).map(toInsert -> exchange(toInsert, ColumnMapRowMapper.INSTANCE));
		}

		private <R> SqlResult<R> exchange(Object toInsert, BiFunction<Row, RowMetadata, R> mappingFunction) {

			StringBuilder builder = new StringBuilder();

			List<Pair<String, Object>> insertValues = dataAccessStrategy.getInsert(toInsert);
			String fieldNames = insertValues.stream().map(Pair::getFirst).collect(Collectors.joining(","));
			String placeholders = IntStream.range(0, insertValues.size()).mapToObj(i -> "$" + (i + 1))
					.collect(Collectors.joining(","));

			builder.append("INSERT INTO ").append(table).append(" (").append(fieldNames).append(") ").append(" VALUES(")
					.append(placeholders).append(")");

			String sql = builder.toString();

			Function<Connection, Statement> insertFunction = it -> {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing SQL statement [" + sql + "]");
				}

				Statement statement = it.createStatement(sql);

				AtomicInteger index = new AtomicInteger();

				for (Pair<String, Object> pair : insertValues) {

					if (pair.getSecond() != null) { // TODO: Better type to transport null values.
						statement.bind(index.getAndIncrement(), pair.getSecond());
					} else {
						statement.bindNull("$" + (index.getAndIncrement() + 1), 0); // TODO: What is type?
					}
				}

				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = it -> Flux
					.from(insertFunction.apply(it).executeReturningGeneratedKeys());

			return new DefaultSqlResultFunctions<>(sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}
	}

	/**
	 * Default {@link org.springframework.data.jdbc.core.function.SqlResult} implementation.
	 */
	class DefaultSqlResultFunctions<T> implements SqlResult<T> {

		private final String sql;
		private final Function<Connection, Flux<Result>> resultFunction;
		private final Function<Connection, Mono<Integer>> updatedRowsFunction;
		private final FetchSpec<T> fetchSpec;

		DefaultSqlResultFunctions(String sql, Function<Connection, Flux<Result>> resultFunction,
				Function<Connection, Mono<Integer>> updatedRowsFunction, BiFunction<Row, RowMetadata, T> mappingFunction) {

			this.sql = sql;
			this.resultFunction = resultFunction;
			this.updatedRowsFunction = updatedRowsFunction;

			this.fetchSpec = new DefaultFetchFunctions<>(sql,
					it -> resultFunction.apply(it).flatMap(result -> result.map(mappingFunction)), updatedRowsFunction);
		}

		@Override
		public <R> SqlResult<R> extract(BiFunction<Row, RowMetadata, R> mappingFunction) {
			return new DefaultSqlResultFunctions<>(sql, resultFunction, updatedRowsFunction, mappingFunction);
		}

		@Override
		public Mono<T> one() {
			return fetchSpec.one();
		}

		@Override
		public Mono<T> first() {
			return fetchSpec.first();
		}

		@Override
		public Flux<T> all() {
			return fetchSpec.all();
		}

		@Override
		public Mono<Integer> rowsUpdated() {
			return fetchSpec.rowsUpdated();
		}
	}

	@RequiredArgsConstructor
	class DefaultFetchFunctions<T> implements FetchSpec<T> {

		private final String sql;
		private final Function<Connection, Flux<T>> resultFunction;
		private final Function<Connection, Mono<Integer>> updatedRowsFunction;

		@Override
		public Mono<T> one() {

			return all().buffer(2) //
					.flatMap(it -> {

						if (it.isEmpty()) {
							return Mono.empty();
						}

						if (it.size() > 1) {
							return Mono.error(new IncorrectResultSizeDataAccessException(
									String.format("Query [%s] returned non unique result.", this.sql), 1));
						}

						return Mono.just(it.get(0));
					}).next();
		}

		@Override
		public Mono<T> first() {
			return all().next();
		}

		@Override
		public Flux<T> all() {
			return executeMany(resultFunction);
		}

		@Override
		public Mono<Integer> rowsUpdated() {
			return execute(updatedRowsFunction);
		}
	}

	private static <T> Flux<T> doInConnectionMany(Connection connection, Function<Connection, Flux<T>> action) {

		try {
			return action.apply(connection);
		} catch (RuntimeException e) {

			String sql = getSql(action);
			return Flux.error(new DefaultDatabaseClient.UncategorizedSQLException("ConnectionCallback", sql, e) {});
		}
	}

	private static <T> Mono<T> doInConnection(Connection connection, Function<Connection, Mono<T>> action) {

		try {
			return action.apply(connection);
		} catch (RuntimeException e) {

			String sql = getSql(action);
			return Mono.error(new DefaultDatabaseClient.UncategorizedSQLException("ConnectionCallback", sql, e) {});
		}
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

		CloseSuppressingInvocationHandler(Connection target) {
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

	private static class UncategorizedSQLException extends UncategorizedDataAccessException implements SqlProvider {

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
