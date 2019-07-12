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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;
import org.springframework.data.r2dbc.connectionfactory.ConnectionFactoryUtils;
import org.springframework.data.r2dbc.connectionfactory.ConnectionProxy;
import org.springframework.data.r2dbc.convert.ColumnMapRowMapper;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.r2dbc.query.Update;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
class DefaultDatabaseClient implements DatabaseClient, ConnectionAccessor {

	private final Log logger = LogFactory.getLog(getClass());

	private final ConnectionFactory connector;

	private final R2dbcExceptionTranslator exceptionTranslator;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final boolean namedParameters;

	private final DefaultDatabaseClientBuilder builder;

	DefaultDatabaseClient(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
			ReactiveDataAccessStrategy dataAccessStrategy, boolean namedParameters, DefaultDatabaseClientBuilder builder) {

		this.connector = connector;
		this.exceptionTranslator = exceptionTranslator;
		this.dataAccessStrategy = dataAccessStrategy;
		this.namedParameters = namedParameters;
		this.builder = builder;
	}

	@Override
	public Builder mutate() {
		return this.builder;
	}

	@Override
	public SqlSpec execute() {
		return new DefaultSqlSpec();
	}

	@Override
	public SelectFromSpec select() {
		return new DefaultSelectFromSpec();
	}

	@Override
	public InsertIntoSpec insert() {
		return new DefaultInsertIntoSpec();
	}

	@Override
	public UpdateTableSpec update() {
		return new DefaultUpdateTableSpec();
	}

	@Override
	public DeleteFromSpec delete() {
		return new DefaultDeleteFromSpec();
	}

	@Override
	public GenericExecuteSpec execute(String sql) {

		Assert.hasText(sql, "SQL must not be null or empty!");

		return execute(() -> sql);
	}

	@Override
	public GenericExecuteSpec execute(Supplier<String> sqlSupplier) {

		Assert.notNull(sqlSupplier, "SQL Supplier must not be null!");

		return createGenericExecuteSpec(sqlSupplier);
	}

	/**
	 * Execute a callback {@link Function} within a {@link Connection} scope. The function is responsible for creating a
	 * {@link Mono}. The connection is released after the {@link Mono} terminates (or the subscription is cancelled).
	 * Connection resources must not be passed outside of the {@link Function} closure, otherwise resources may get
	 * defunct.
	 *
	 * @param action must not be {@literal null}.
	 * @return the resulting {@link Mono}.
	 * @throws DataAccessException when during construction of the {@link Mono} a problem occurs.
	 */
	@Override
	public <T> Mono<T> inConnection(Function<Connection, Mono<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<ConnectionCloseHolder> connectionMono = getConnection()
				.map(it -> new ConnectionCloseHolder(it, this::closeConnection));

		return Mono.usingWhen(connectionMono, it -> {

			// Create close-suppressing Connection proxy
			Connection connectionToUse = createConnectionProxy(it.connection);

			return doInConnection(connectionToUse, action);
		}, ConnectionCloseHolder::close, ConnectionCloseHolder::close, ConnectionCloseHolder::close) //
				.onErrorMap(R2dbcException.class, ex -> translateException("execute", getSql(action), ex));
	}

	/**
	 * Execute a callback {@link Function} within a {@link Connection} scope. The function is responsible for creating a
	 * {@link Flux}. The connection is released after the {@link Flux} terminates (or the subscription is cancelled).
	 * Connection resources must not be passed outside of the {@link Function} closure, otherwise resources may get
	 * defunct.
	 *
	 * @param action must not be {@literal null}.
	 * @return the resulting {@link Flux}.
	 * @throws DataAccessException when during construction of the {@link Mono} a problem occurs.
	 */
	@Override
	public <T> Flux<T> inConnectionMany(Function<Connection, Flux<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<ConnectionCloseHolder> connectionMono = getConnection()
				.map(it -> new ConnectionCloseHolder(it, this::closeConnection));

		return Flux.usingWhen(connectionMono, it -> {

			// Create close-suppressing Connection proxy, also preparing returned Statements.
			Connection connectionToUse = createConnectionProxy(it.connection);

			return doInConnectionMany(connectionToUse, action);
		}, ConnectionCloseHolder::close, ConnectionCloseHolder::close, ConnectionCloseHolder::close) //
				.onErrorMap(R2dbcException.class, ex -> translateException("executeMany", getSql(action), ex));
	}

	/**
	 * Obtain a {@link Connection}.
	 *
	 * @return a {@link Mono} able to emit a {@link Connection}.
	 */
	protected Mono<Connection> getConnection() {
		return ConnectionFactoryUtils.getConnection(obtainConnectionFactory()).map(Tuple2::getT1);
	}

	/**
	 * Release the {@link Connection}.
	 *
	 * @param connection to close.
	 * @return a {@link Publisher} that completes successfully when the connection is closed.
	 */
	protected Publisher<Void> closeConnection(Connection connection) {

		return ConnectionFactoryUtils.currentConnectionFactory(obtainConnectionFactory()).then()
				.onErrorResume(Exception.class, e -> Mono.from(connection.close()));
	}

	/**
	 * Obtain the {@link ConnectionFactory} for actual use.
	 *
	 * @return the ConnectionFactory (never {@literal null})
	 * @throws IllegalStateException in case of no DataSource set
	 */
	protected ConnectionFactory obtainConnectionFactory() {
		return this.connector;
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
	 * Translate the given {@link R2dbcException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param sql SQL query or update that caused the problem (may be {@literal null}).
	 * @param ex the offending {@link R2dbcException}.
	 * @return a DataAccessException wrapping the {@link R2dbcException} (never {@literal null}).
	 */
	protected DataAccessException translateException(String task, @Nullable String sql, R2dbcException ex) {

		DataAccessException dae = this.exceptionTranslator.translate(task, sql, ex);
		return dae != null ? dae : new UncategorizedR2dbcException(task, sql, ex);
	}

	/**
	 * Customization hook.
	 */
	protected <T> DefaultTypedExecuteSpec<T> createTypedExecuteSpec(Map<Integer, SettableValue> byIndex,
			Map<String, SettableValue> byName, Supplier<String> sqlSupplier, Class<T> typeToRead) {
		return new DefaultTypedExecuteSpec<>(byIndex, byName, sqlSupplier, typeToRead);
	}

	/**
	 * Customization hook.
	 */
	protected <T> DefaultTypedExecuteSpec<T> createTypedExecuteSpec(Map<Integer, SettableValue> byIndex,
			Map<String, SettableValue> byName, Supplier<String> sqlSupplier,
			BiFunction<Row, RowMetadata, T> mappingFunction) {
		return new DefaultTypedExecuteSpec<>(byIndex, byName, sqlSupplier, mappingFunction);
	}

	/**
	 * Customization hook.
	 */
	protected ExecuteSpecSupport createGenericExecuteSpec(Map<Integer, SettableValue> byIndex,
			Map<String, SettableValue> byName, Supplier<String> sqlSupplier) {
		return new DefaultGenericExecuteSpec(byIndex, byName, sqlSupplier);
	}

	/**
	 * Customization hook.
	 */
	protected DefaultGenericExecuteSpec createGenericExecuteSpec(Supplier<String> sqlSupplier) {
		return new DefaultGenericExecuteSpec(sqlSupplier);
	}

	private static void bindByName(Statement statement, Map<String, SettableValue> byName) {

		byName.forEach((name, o) -> {

			if (o.getValue() != null) {
				statement.bind(name, o.getValue());
			} else {
				statement.bindNull(name, o.getType());
			}
		});
	}

	private static void bindByIndex(Statement statement, Map<Integer, SettableValue> byIndex) {

		byIndex.forEach((i, o) -> {

			if (o.getValue() != null) {
				statement.bind(i.intValue(), o.getValue());
			} else {
				statement.bindNull(i.intValue(), o.getType());
			}
		});
	}

	/**
	 * Default {@link DatabaseClient.SqlSpec} implementation.
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

			return createGenericExecuteSpec(sqlSupplier);
		}
	}

	/**
	 * Base class for {@link DatabaseClient.GenericExecuteSpec} implementations.
	 */
	@RequiredArgsConstructor
	class ExecuteSpecSupport {

		final Map<Integer, SettableValue> byIndex;
		final Map<String, SettableValue> byName;
		final Supplier<String> sqlSupplier;

		ExecuteSpecSupport(Supplier<String> sqlSupplier) {

			this.byIndex = Collections.emptyMap();
			this.byName = Collections.emptyMap();
			this.sqlSupplier = sqlSupplier;
		}

		<T> FetchSpec<T> exchange(Supplier<String> sqlSupplier, BiFunction<Row, RowMetadata, T> mappingFunction) {

			String sql = getRequiredSql(sqlSupplier);

			Function<Connection, Statement> executeFunction = it -> {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing SQL statement [" + sql + "]");
				}

				if (sqlSupplier instanceof PreparedOperation<?>) {

					Statement statement = it.createStatement(sql);
					BindTarget bindTarget = new StatementWrapper(statement);
					((PreparedOperation<?>) sqlSupplier).bindTo(bindTarget);

					return statement;
				}

				if (namedParameters) {

					PreparedOperation<?> operation = dataAccessStrategy.processNamedParameters(sql, this.byName);

					String expanded = getRequiredSql(operation);
					if (logger.isTraceEnabled()) {
						logger.trace("Expanded SQL [" + expanded + "]");
					}

					Statement statement = it.createStatement(expanded);
					BindTarget bindTarget = new StatementWrapper(statement);

					operation.bindTo(bindTarget);
					bindByIndex(statement, this.byIndex);

					return statement;
				}

				Statement statement = it.createStatement(sql);

				bindByIndex(statement, this.byIndex);
				bindByName(statement, this.byName);

				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(executeFunction.apply(it).execute());

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}

		public ExecuteSpecSupport bind(int index, Object value) {

			assertNotPreparedOperation();
			Assert.notNull(value, () -> String.format("Value at index %d must not be null. Use bindNull(…) instead.", index));

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, SettableValue.fromOrEmpty(value, value.getClass()));

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public ExecuteSpecSupport bindNull(int index, Class<?> type) {

			assertNotPreparedOperation();

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, SettableValue.empty(type));

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public ExecuteSpecSupport bind(String name, Object value) {

			assertNotPreparedOperation();

			Assert.hasText(name, "Parameter name must not be null or empty!");
			Assert.notNull(value,
					() -> String.format("Value for parameter %s must not be null. Use bindNull(…) instead.", name));

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, SettableValue.fromOrEmpty(value, value.getClass()));

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		public ExecuteSpecSupport bindNull(String name, Class<?> type) {

			assertNotPreparedOperation();
			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, SettableValue.empty(type));

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		private void assertNotPreparedOperation() {
			if (this.sqlSupplier instanceof PreparedOperation<?>) {
				throw new InvalidDataAccessApiUsageException("Cannot add bindings to a PreparedOperation");
			}
		}

		protected ExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier) {
			return new ExecuteSpecSupport(byIndex, byName, sqlSupplier);
		}

		public ExecuteSpecSupport bind(Object bean) {

			Assert.notNull(bean, "Bean must not be null!");

			throw new UnsupportedOperationException("Implement me!");
		}
	}

	/**
	 * Default {@link DatabaseClient.GenericExecuteSpec} implementation.
	 */
	protected class DefaultGenericExecuteSpec extends ExecuteSpecSupport implements GenericExecuteSpec {

		DefaultGenericExecuteSpec(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier) {
			super(byIndex, byName, sqlSupplier);
		}

		DefaultGenericExecuteSpec(Supplier<String> sqlSupplier) {
			super(sqlSupplier);
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return createTypedExecuteSpec(this.byIndex, this.byName, this.sqlSupplier, resultType);
		}

		@Override
		public <R> FetchSpec<R> map(Function<Row, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(this.sqlSupplier, (row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <R> FetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(this.sqlSupplier, mappingFunction);
		}

		@Override
		public FetchSpec<Map<String, Object>> fetch() {
			return exchange(this.sqlSupplier, ColumnMapRowMapper.INSTANCE);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		@Override
		public DefaultGenericExecuteSpec bind(int index, Object value) {
			return (DefaultGenericExecuteSpec) super.bind(index, value);
		}

		@Override
		public DefaultGenericExecuteSpec bindNull(int index, Class<?> type) {
			return (DefaultGenericExecuteSpec) super.bindNull(index, type);
		}

		@Override
		public DefaultGenericExecuteSpec bind(String name, Object value) {
			return (DefaultGenericExecuteSpec) super.bind(name, value);
		}

		@Override
		public DefaultGenericExecuteSpec bindNull(String name, Class<?> type) {
			return (DefaultGenericExecuteSpec) super.bindNull(name, type);
		}

		@Override
		public DefaultGenericExecuteSpec bind(Object bean) {
			return (DefaultGenericExecuteSpec) super.bind(bean);
		}

		@Override
		protected ExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier) {
			return createGenericExecuteSpec(byIndex, byName, sqlSupplier);
		}
	}

	/**
	 * Default {@link DatabaseClient.GenericExecuteSpec} implementation.
	 */
	@SuppressWarnings("unchecked")
	protected class DefaultTypedExecuteSpec<T> extends ExecuteSpecSupport implements TypedExecuteSpec<T> {

		private final Class<T> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedExecuteSpec(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, Class<T> typeToRead) {

			super(byIndex, byName, sqlSupplier);

			this.typeToRead = typeToRead;
			this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
		}

		DefaultTypedExecuteSpec(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, BiFunction<Row, RowMetadata, T> mappingFunction) {

			super(byIndex, byName, sqlSupplier);

			this.typeToRead = null;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return createTypedExecuteSpec(this.byIndex, this.byName, this.sqlSupplier, resultType);
		}

		@Override
		public <R> FetchSpec<R> map(Function<Row, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(this.sqlSupplier, (row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <R> FetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(this.sqlSupplier, mappingFunction);
		}

		@Override
		public FetchSpec<T> fetch() {
			return exchange(this.sqlSupplier, this.mappingFunction);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		@Override
		public DefaultTypedExecuteSpec<T> bind(int index, Object value) {
			return (DefaultTypedExecuteSpec<T>) super.bind(index, value);
		}

		@Override
		public DefaultTypedExecuteSpec<T> bindNull(int index, Class<?> type) {
			return (DefaultTypedExecuteSpec<T>) super.bindNull(index, type);
		}

		@Override
		public DefaultTypedExecuteSpec<T> bind(String name, Object value) {
			return (DefaultTypedExecuteSpec<T>) super.bind(name, value);
		}

		@Override
		public DefaultTypedExecuteSpec<T> bindNull(String name, Class<?> type) {
			return (DefaultTypedExecuteSpec<T>) super.bindNull(name, type);
		}

		@Override
		public DefaultTypedExecuteSpec<T> bind(Object bean) {
			return (DefaultTypedExecuteSpec<T>) super.bind(bean);
		}

		@Override
		protected DefaultTypedExecuteSpec<T> createInstance(Map<Integer, SettableValue> byIndex,
				Map<String, SettableValue> byName, Supplier<String> sqlSupplier) {
			return createTypedExecuteSpec(byIndex, byName, sqlSupplier, this.typeToRead);
		}
	}

	/**
	 * Default {@link DatabaseClient.SelectFromSpec} implementation.
	 */
	class DefaultSelectFromSpec implements SelectFromSpec {

		@Override
		public GenericSelectSpec from(String table) {
			return new DefaultGenericSelectSpec(table);
		}

		@Override
		public <T> TypedSelectSpec<T> from(Class<T> table) {
			return new DefaultTypedSelectSpec<>(table);
		}
	}

	/**
	 * Base class for {@link DatabaseClient.GenericExecuteSpec} implementations.
	 */
	@RequiredArgsConstructor
	private abstract class DefaultSelectSpecSupport {

		final String table;
		final List<String> projectedFields;
		final @Nullable Criteria criteria;
		final Sort sort;
		final Pageable page;

		DefaultSelectSpecSupport(String table) {

			Assert.hasText(table, "Table name must not be null!");

			this.table = table;
			this.projectedFields = Collections.emptyList();
			this.criteria = null;
			this.sort = Sort.unsorted();
			this.page = Pageable.unpaged();
		}

		public DefaultSelectSpecSupport project(String... selectedFields) {
			Assert.notNull(selectedFields, "Projection fields must not be null!");

			List<String> projectedFields = new ArrayList<>(this.projectedFields.size() + selectedFields.length);
			projectedFields.addAll(this.projectedFields);
			projectedFields.addAll(Arrays.asList(selectedFields));

			return createInstance(this.table, projectedFields, this.criteria, this.sort, this.page);
		}

		public DefaultSelectSpecSupport where(Criteria whereCriteria) {

			Assert.notNull(whereCriteria, "Criteria must not be null!");

			return createInstance(this.table, this.projectedFields, whereCriteria, this.sort, this.page);
		}

		public DefaultSelectSpecSupport orderBy(Sort sort) {

			Assert.notNull(sort, "Sort must not be null!");

			return createInstance(this.table, this.projectedFields, this.criteria, sort, this.page);
		}

		public DefaultSelectSpecSupport page(Pageable page) {

			Assert.notNull(page, "Pageable must not be null!");

			return createInstance(this.table, this.projectedFields, this.criteria, this.sort, page);
		}

		<R> FetchSpec<R> execute(PreparedOperation<?> preparedOperation, BiFunction<Row, RowMetadata, R> mappingFunction) {

			String sql = getRequiredSql(preparedOperation);
			Function<Connection, Statement> selectFunction = wrapPreparedOperation(sql, preparedOperation);
			Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(selectFunction.apply(it).execute());

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> Mono.error(new UnsupportedOperationException("Not available for SELECT")), //
					mappingFunction);
		}

		protected abstract DefaultSelectSpecSupport createInstance(String table, List<String> projectedFields,
				Criteria criteria, Sort sort, Pageable page);
	}

	private class DefaultGenericSelectSpec extends DefaultSelectSpecSupport implements GenericSelectSpec {

		DefaultGenericSelectSpec(String table, List<String> projectedFields, Criteria criteria, Sort sort, Pageable page) {
			super(table, projectedFields, criteria, sort, page);
		}

		DefaultGenericSelectSpec(String table) {
			super(table);
		}

		@Override
		public <R> TypedSelectSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return new DefaultTypedSelectSpec<>(this.table, this.projectedFields, this.criteria, this.sort, this.page,
					resultType, dataAccessStrategy.getRowMapper(resultType));
		}

		@Override
		public <R> FetchSpec<R> map(Function<Row, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange((row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <R> FetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(mappingFunction);
		}

		@Override
		public DefaultGenericSelectSpec project(String... selectedFields) {
			return (DefaultGenericSelectSpec) super.project(selectedFields);
		}

		@Override
		public DefaultGenericSelectSpec matching(Criteria criteria) {
			return (DefaultGenericSelectSpec) super.where(criteria);
		}

		@Override
		public DefaultGenericSelectSpec orderBy(Sort sort) {
			return (DefaultGenericSelectSpec) super.orderBy(sort);
		}

		@Override
		public DefaultGenericSelectSpec page(Pageable pageable) {
			return (DefaultGenericSelectSpec) super.page(pageable);
		}

		@Override
		public FetchSpec<Map<String, Object>> fetch() {
			return exchange(ColumnMapRowMapper.INSTANCE);
		}

		private <R> FetchSpec<R> exchange(BiFunction<Row, RowMetadata, R> mappingFunction) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();

			StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.table).withProjection(this.projectedFields)
					.withSort(this.sort).withPage(this.page);

			if (this.criteria != null) {
				selectSpec = selectSpec.withCriteria(this.criteria);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);
			return execute(operation, mappingFunction);
		}

		@Override
		protected DefaultGenericSelectSpec createInstance(String table, List<String> projectedFields, Criteria criteria,
				Sort sort, Pageable page) {
			return new DefaultGenericSelectSpec(table, projectedFields, criteria, sort, page);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	@SuppressWarnings("unchecked")
	private class DefaultTypedSelectSpec<T> extends DefaultSelectSpecSupport implements TypedSelectSpec<T> {

		private final @Nullable Class<T> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedSelectSpec(Class<T> typeToRead) {

			super(dataAccessStrategy.getTableName(typeToRead));

			this.typeToRead = typeToRead;
			this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
		}

		DefaultTypedSelectSpec(String table, List<String> projectedFields, Criteria criteria, Sort sort, Pageable page,
				Class<T> typeToRead, BiFunction<Row, RowMetadata, T> mappingFunction) {

			super(table, projectedFields, criteria, sort, page);

			this.typeToRead = typeToRead;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public <R> FetchSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return exchange(dataAccessStrategy.getRowMapper(resultType));
		}

		@Override
		public <R> FetchSpec<R> map(Function<Row, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange((row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <R> FetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(mappingFunction);
		}

		@Override
		public DefaultTypedSelectSpec<T> project(String... selectedFields) {
			return (DefaultTypedSelectSpec<T>) super.project(selectedFields);
		}

		@Override
		public DefaultTypedSelectSpec<T> matching(Criteria criteria) {
			return (DefaultTypedSelectSpec<T>) super.where(criteria);
		}

		@Override
		public DefaultTypedSelectSpec<T> orderBy(Sort sort) {
			return (DefaultTypedSelectSpec<T>) super.orderBy(sort);
		}

		@Override
		public DefaultTypedSelectSpec<T> page(Pageable pageable) {
			return (DefaultTypedSelectSpec<T>) super.page(pageable);
		}

		@Override
		public FetchSpec<T> fetch() {
			return exchange(this.mappingFunction);
		}

		private <R> FetchSpec<R> exchange(BiFunction<Row, RowMetadata, R> mappingFunction) {

			List<String> columns;
			StatementMapper mapper = dataAccessStrategy.getStatementMapper().forType(this.typeToRead);

			if (this.projectedFields.isEmpty()) {
				columns = dataAccessStrategy.getAllColumns(this.typeToRead);
			} else {
				columns = this.projectedFields;
			}

			StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.table).withProjection(columns)
					.withPage(this.page).withSort(this.sort);

			if (this.criteria != null) {
				selectSpec = selectSpec.withCriteria(this.criteria);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);

			return execute(operation, mappingFunction);
		}

		@Override
		protected DefaultTypedSelectSpec<T> createInstance(String table, List<String> projectedFields, Criteria criteria,
				Sort sort, Pageable page) {
			return new DefaultTypedSelectSpec<>(table, projectedFields, criteria, sort, page, this.typeToRead,
					this.mappingFunction);
		}
	}

	/**
	 * Default {@link DatabaseClient.InsertIntoSpec} implementation.
	 */
	class DefaultInsertIntoSpec implements InsertIntoSpec {

		@Override
		public GenericInsertSpec<Map<String, Object>> into(String table) {
			return new DefaultGenericInsertSpec<>(table, Collections.emptyMap(), ColumnMapRowMapper.INSTANCE);
		}

		@Override
		public <T> TypedInsertSpec<T> into(Class<T> table) {
			return new DefaultTypedInsertSpec<>(table, ColumnMapRowMapper.INSTANCE);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.GenericInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultGenericInsertSpec<T> implements GenericInsertSpec<T> {

		private final String table;
		private final Map<String, SettableValue> byName;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		@Override
		public GenericInsertSpec<T> value(String field, Object value) {

			Assert.notNull(field, "Field must not be null!");
			Assert.notNull(value,
					() -> String.format("Value for field %s must not be null. Use nullValue(…) instead.", field));

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);

			if (value instanceof SettableValue) {
				byName.put(field, (SettableValue) value);
			} else {
				byName.put(field, SettableValue.fromOrEmpty(value, value.getClass()));
			}

			return new DefaultGenericInsertSpec<>(this.table, byName, this.mappingFunction);
		}

		@Override
		public GenericInsertSpec<T> nullValue(String field) {

			Assert.notNull(field, "Field must not be null!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(field, null);

			return new DefaultGenericInsertSpec<>(this.table, byName, this.mappingFunction);
		}

		@Override
		public <R> FetchSpec<R> map(Function<Row, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange((row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <R> FetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(mappingFunction);
		}

		@Override
		public FetchSpec<T> fetch() {
			return exchange(this.mappingFunction);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		private <R> FetchSpec<R> exchange(BiFunction<Row, RowMetadata, R> mappingFunction) {

			if (this.byName.isEmpty()) {
				throw new IllegalStateException("Insert fields is empty!");
			}

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();
			StatementMapper.InsertSpec insert = mapper.createInsert(this.table);

			for (String column : this.byName.keySet()) {
				insert = insert.withColumn(column, this.byName.get(column));
			}

			PreparedOperation<?> operation = mapper.getMappedObject(insert);
			return exchangeInsert(mappingFunction, operation);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultTypedInsertSpec<T, R> implements TypedInsertSpec<T>, InsertSpec<R> {

		private final Class<?> typeToInsert;
		private final String table;
		private final Publisher<T> objectToInsert;
		private final BiFunction<Row, RowMetadata, R> mappingFunction;

		DefaultTypedInsertSpec(Class<?> typeToInsert, BiFunction<Row, RowMetadata, R> mappingFunction) {

			this.typeToInsert = typeToInsert;
			this.table = dataAccessStrategy.getTableName(typeToInsert);
			this.objectToInsert = Mono.empty();
			this.mappingFunction = mappingFunction;
		}

		@Override
		public TypedInsertSpec<T> table(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty!");

			return new DefaultTypedInsertSpec<>(this.typeToInsert, tableName, this.objectToInsert, this.mappingFunction);
		}

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public InsertSpec using(T objectToInsert) {

			Assert.notNull(objectToInsert, "Object to insert must not be null!");

			return new DefaultTypedInsertSpec<>(this.typeToInsert, this.table, Mono.just(objectToInsert),
					this.mappingFunction);
		}

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public InsertSpec using(Publisher<T> objectToInsert) {

			Assert.notNull(objectToInsert, "Publisher to insert must not be null!");

			return new DefaultTypedInsertSpec<>(this.typeToInsert, this.table, objectToInsert, this.mappingFunction);
		}

		@Override
		public <MR> FetchSpec<MR> map(Function<Row, MR> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange((row, rowMetadata) -> mappingFunction.apply(row));
		}

		@Override
		public <MR> FetchSpec<MR> map(BiFunction<Row, RowMetadata, MR> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return exchange(mappingFunction);
		}

		@Override
		public FetchSpec<R> fetch() {
			return exchange(this.mappingFunction);
		}

		@Override
		public Mono<Void> then() {
			return Mono.from(this.objectToInsert).flatMapMany(toInsert -> exchange(toInsert, (row, md) -> row).all()).then();
		}

		private <MR> FetchSpec<MR> exchange(BiFunction<Row, RowMetadata, MR> mappingFunction) {

			return new FetchSpec<MR>() {
				@Override
				public Mono<MR> one() {
					return Mono.from(objectToInsert).flatMap(toInsert -> exchange(toInsert, mappingFunction).one());
				}

				@Override
				public Mono<MR> first() {
					return Mono.from(objectToInsert).flatMap(toInsert -> exchange(toInsert, mappingFunction).first());
				}

				@Override
				public Flux<MR> all() {
					return Flux.from(objectToInsert).flatMap(toInsert -> exchange(toInsert, mappingFunction).all());
				}

				@Override
				public Mono<Integer> rowsUpdated() {
					return Mono.from(objectToInsert).flatMapMany(toInsert -> exchange(toInsert, mappingFunction).rowsUpdated())
							.collect(Collectors.summingInt(Integer::intValue));
				}
			};
		}

		private <MR> FetchSpec<MR> exchange(Object toInsert, BiFunction<Row, RowMetadata, MR> mappingFunction) {

			OutboundRow outboundRow = dataAccessStrategy.getOutboundRow(toInsert);

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();
			StatementMapper.InsertSpec insert = mapper.createInsert(this.table);

			for (String column : outboundRow.keySet()) {
				SettableValue settableValue = outboundRow.get(column);
				if (settableValue.hasValue()) {
					insert = insert.withColumn(column, settableValue);
				}
			}

			PreparedOperation<?> operation = mapper.getMappedObject(insert);
			return exchangeInsert(mappingFunction, operation);
		}
	}

	/**
	 * Default {@link DatabaseClient.UpdateTableSpec} implementation.
	 */
	class DefaultUpdateTableSpec implements UpdateTableSpec {

		@Override
		public GenericUpdateSpec table(String table) {
			return new DefaultGenericUpdateSpec(null, table, null, null);
		}

		@Override
		public <T> TypedUpdateSpec<T> table(Class<T> table) {
			return new DefaultTypedUpdateSpec<>(table, null, null);
		}
	}

	@RequiredArgsConstructor
	class DefaultGenericUpdateSpec implements GenericUpdateSpec, UpdateMatchingSpec {

		private final @Nullable Class<?> typeToUpdate;
		private final @Nullable String table;
		private final Update assignments;
		private final Criteria where;

		@Override
		public UpdateMatchingSpec using(Update update) {

			Assert.notNull(update, "Update must not be null");

			return new DefaultGenericUpdateSpec(this.typeToUpdate, this.table, update, this.where);
		}

		@Override
		public UpdateSpec matching(Criteria criteria) {

			Assert.notNull(criteria, "Criteria must not be null");

			return new DefaultGenericUpdateSpec(this.typeToUpdate, this.table, this.assignments, criteria);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			String table;

			if (StringUtils.isEmpty(this.table)) {
				table = dataAccessStrategy.getTableName(this.typeToUpdate);
			} else {
				table = this.table;
			}

			return exchange(table);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		private UpdatedRowsFetchSpec exchange(String table) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();

			if (this.typeToUpdate != null) {
				mapper = mapper.forType(this.typeToUpdate);
			}

			StatementMapper.UpdateSpec update = mapper.createUpdate(table, this.assignments);

			if (this.where != null) {
				update = update.withCriteria(this.where);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(update);

			return exchangeUpdate(operation);
		}
	}

	@RequiredArgsConstructor
	class DefaultTypedUpdateSpec<T> implements TypedUpdateSpec<T>, UpdateSpec {

		private final @Nullable Class<T> typeToUpdate;
		private final @Nullable String table;
		private final T objectToUpdate;

		@Override
		public UpdateSpec using(T objectToUpdate) {

			Assert.notNull(objectToUpdate, "Object to update must not be null");

			return new DefaultTypedUpdateSpec<>(this.typeToUpdate, this.table, objectToUpdate);
		}

		@Override
		public TypedUpdateSpec<T> table(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty!");

			return new DefaultTypedUpdateSpec<>(this.typeToUpdate, tableName, this.objectToUpdate);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			String table;

			if (StringUtils.isEmpty(this.table)) {
				table = dataAccessStrategy.getTableName(this.typeToUpdate);
			} else {
				table = this.table;
			}

			return exchange(table);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		private UpdatedRowsFetchSpec exchange(String table) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();
			Map<String, SettableValue> columns = dataAccessStrategy.getOutboundRow(this.objectToUpdate);
			List<String> ids = dataAccessStrategy.getIdentifierColumns(this.typeToUpdate);

			if (ids.isEmpty()) {
				throw new IllegalStateException("No identifier columns in " + this.typeToUpdate.getName() + "!");
			}
			Object id = columns.remove(ids.get(0)); // do not update the Id column.

			Update update = null;

			for (String column : columns.keySet()) {
				if (update == null) {
					update = Update.update(column, columns.get(column));
				} else {
					update = update.set(column, columns.get(column));
				}
			}

			PreparedOperation<?> operation = mapper
					.getMappedObject(mapper.createUpdate(table, update).withCriteria(Criteria.where(ids.get(0)).is(id)));

			return exchangeUpdate(operation);
		}
	}

	/**
	 * Default {@link DatabaseClient.DeleteFromSpec} implementation.
	 */
	class DefaultDeleteFromSpec implements DeleteFromSpec {

		@Override
		public DefaultDeleteSpec<?> from(String table) {
			return new DefaultDeleteSpec<>(null, table, null);
		}

		@Override
		public <T> DefaultDeleteSpec<T> from(Class<T> table) {
			return new DefaultDeleteSpec<>(table, null, null);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultDeleteSpec<T> implements DeleteMatchingSpec, TypedDeleteSpec<T> {

		private final @Nullable Class<T> typeToDelete;
		private final @Nullable String table;
		private final Criteria where;

		@Override
		public DeleteSpec matching(Criteria criteria) {

			Assert.notNull(criteria, "Criteria must not be null!");

			return new DefaultDeleteSpec<>(this.typeToDelete, this.table, criteria);
		}

		@Override
		public TypedDeleteSpec<T> table(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty!");

			return new DefaultDeleteSpec<>(this.typeToDelete, tableName, this.where);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			String table;

			if (StringUtils.isEmpty(this.table)) {
				table = dataAccessStrategy.getTableName(this.typeToDelete);
			} else {
				table = this.table;
			}

			return exchange(table);
		}

		@Override
		public Mono<Void> then() {
			return fetch().rowsUpdated().then();
		}

		private UpdatedRowsFetchSpec exchange(String table) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();

			if (this.typeToDelete != null) {
				mapper = mapper.forType(this.typeToDelete);
			}

			StatementMapper.DeleteSpec delete = mapper.createDelete(table);

			if (this.where != null) {
				delete = delete.withCriteria(this.where);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(delete);

			return exchangeUpdate(operation);
		}
	}

	private <R> FetchSpec<R> exchangeInsert(BiFunction<Row, RowMetadata, R> mappingFunction,
			PreparedOperation<?> operation) {

		String sql = getRequiredSql(operation);
		Function<Connection, Statement> insertFunction = wrapPreparedOperation(sql, operation)
				.andThen(statement -> statement.returnGeneratedValues());
		Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(insertFunction.apply(it).execute());

		return new DefaultSqlResult<>(this, //
				sql, //
				resultFunction, //
				it -> sumRowsUpdated(resultFunction, it), //
				mappingFunction);
	}

	private UpdatedRowsFetchSpec exchangeUpdate(PreparedOperation<?> operation) {

		String sql = getRequiredSql(operation);
		Function<Connection, Statement> executeFunction = wrapPreparedOperation(sql, operation);
		Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(executeFunction.apply(it).execute());

		return new DefaultSqlResult<>(this, //
				sql, //
				resultFunction, //
				it -> sumRowsUpdated(resultFunction, it), //
				(row, rowMetadata) -> rowMetadata);
	}

	private static Mono<Integer> sumRowsUpdated(Function<Connection, Flux<Result>> resultFunction, Connection it) {

		return resultFunction.apply(it) //
				.flatMap(Result::getRowsUpdated) //
				.collect(Collectors.summingInt(Integer::intValue));
	}

	private Function<Connection, Statement> wrapPreparedOperation(String sql, PreparedOperation<?> operation) {

		return it -> {

			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Executing SQL statement [" + sql + "]");
			}

			Statement statement = it.createStatement(sql);
			operation.bindTo(new StatementWrapper(statement));

			return statement;
		};
	}

	private static <T> Flux<T> doInConnectionMany(Connection connection, Function<Connection, Flux<T>> action) {

		try {
			return action.apply(connection);
		} catch (R2dbcException e) {

			String sql = getSql(action);
			return Flux.error(new UncategorizedR2dbcException("doInConnectionMany", sql, e));
		}
	}

	private static <T> Mono<T> doInConnection(Connection connection, Function<Connection, Mono<T>> action) {

		try {
			return action.apply(connection);
		} catch (R2dbcException e) {

			String sql = getSql(action);
			return Mono.error(new UncategorizedR2dbcException("doInConnection", sql, e));
		}
	}

	/**
	 * Determine SQL from potential provider object.
	 *
	 * @param sqlProvider object that's potentially a SqlProvider
	 * @return the SQL string, or {@literal null}
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

	private static String getRequiredSql(Supplier<String> sqlSupplier) {

		String sql = sqlSupplier.get();
		Assert.state(StringUtils.hasText(sql), "SQL returned by SQL supplier must not be empty!");
		return sql;
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
				return proxy == args[0];
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

	/**
	 * Holder for a connection that makes sure the close action is invoked atomically only once.
	 */
	@RequiredArgsConstructor
	static class ConnectionCloseHolder extends AtomicBoolean {

		private static final long serialVersionUID = -8994138383301201380L;

		final Connection connection;
		final Function<Connection, Publisher<Void>> closeFunction;

		Mono<Void> close() {

			return Mono.defer(() -> {

				if (compareAndSet(false, true)) {
					return Mono.from(this.closeFunction.apply(this.connection));
				}

				return Mono.empty();
			});
		}
	}

	@RequiredArgsConstructor
	static class StatementWrapper implements BindTarget {

		final Statement statement;

		@Override
		public void bind(Object identifier, Object value) {
			this.statement.bind(identifier, value);
		}

		@Override
		public void bind(int index, Object value) {
			this.statement.bind(index, value);
		}

		@Override
		public void bindNull(Object identifier, Class<?> type) {
			this.statement.bindNull(identifier, type);
		}

		@Override
		public void bindNull(int index, Class<?> type) {
			this.statement.bindNull(index, type);
		}
	}

}
