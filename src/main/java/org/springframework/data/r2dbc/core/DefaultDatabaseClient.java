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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;
import org.springframework.data.r2dbc.connectionfactory.ConnectionFactoryUtils;
import org.springframework.data.r2dbc.connectionfactory.ConnectionProxy;
import org.springframework.data.r2dbc.convert.ColumnMapRowMapper;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.Update;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @author Bogdan Ilchyshyn
 */
class DefaultDatabaseClient implements DatabaseClient, ConnectionAccessor {

	private final Log logger = LogFactory.getLog(getClass());

	private final ConnectionFactory connector;

	private final R2dbcExceptionTranslator exceptionTranslator;

	private final ExecuteFunction executeFunction;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final boolean namedParameters;

	private final DefaultDatabaseClientBuilder builder;

	private final ProjectionFactory projectionFactory;

	DefaultDatabaseClient(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
			ExecuteFunction executeFunction, ReactiveDataAccessStrategy dataAccessStrategy, boolean namedParameters,
			ProjectionFactory projectionFactory, DefaultDatabaseClientBuilder builder) {

		this.connector = connector;
		this.exceptionTranslator = exceptionTranslator;
		this.executeFunction = executeFunction;
		this.dataAccessStrategy = dataAccessStrategy;
		this.namedParameters = namedParameters;
		this.projectionFactory = projectionFactory;
		this.builder = builder;
	}

	@Override
	public Builder mutate() {
		return this.builder;
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
		}, ConnectionCloseHolder::close, (it, err) -> it.close(), ConnectionCloseHolder::close) //
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
		}, ConnectionCloseHolder::close, (it, err) -> it.close(), ConnectionCloseHolder::close) //
				.onErrorMap(R2dbcException.class, ex -> translateException("executeMany", getSql(action), ex));
	}

	/**
	 * Obtain a {@link Connection}.
	 *
	 * @return a {@link Mono} able to emit a {@link Connection}.
	 */
	protected Mono<Connection> getConnection() {
		return ConnectionFactoryUtils.getConnection(obtainConnectionFactory());
	}

	/**
	 * Obtain the {@link ReactiveDataAccessStrategy}.
	 *
	 * @return a the ReactiveDataAccessStrategy.
	 */
	protected ReactiveDataAccessStrategy getDataAccessStrategy() {
		return dataAccessStrategy;
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
			Map<String, SettableValue> byName, Supplier<String> sqlSupplier, StatementFilterFunction filterFunction,
			Class<T> typeToRead) {
		return new DefaultTypedExecuteSpec<>(byIndex, byName, sqlSupplier, filterFunction, typeToRead);
	}

	/**
	 * Customization hook.
	 */
	protected ExecuteSpecSupport createGenericExecuteSpec(Map<Integer, SettableValue> byIndex,
			Map<String, SettableValue> byName, Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {
		return new DefaultGenericExecuteSpec(byIndex, byName, sqlSupplier, filterFunction);
	}

	/**
	 * Customization hook.
	 */
	protected DefaultGenericExecuteSpec createGenericExecuteSpec(Supplier<String> sqlSupplier) {
		return new DefaultGenericExecuteSpec(sqlSupplier);
	}

	private void bindByName(Statement statement, Map<String, SettableValue> byName) {

		byName.forEach((name, o) -> {

			SettableValue converted = dataAccessStrategy.getBindValue(o);
			if (converted.getValue() != null) {

				statement.bind(name, converted.getValue());
			} else {
				statement.bindNull(name, converted.getType());
			}
		});
	}

	private void bindByIndex(Statement statement, Map<Integer, SettableValue> byIndex) {

		byIndex.forEach((i, o) -> {

			SettableValue converted = dataAccessStrategy.getBindValue(o);
			if (converted.getValue() != null) {
				statement.bind(i, converted.getValue());
			} else {
				statement.bindNull(i, converted.getType());
			}
		});
	}

	/**
	 * Base class for {@link DatabaseClient.GenericExecuteSpec} implementations.
	 */
	class ExecuteSpecSupport {

		final Map<Integer, SettableValue> byIndex;
		final Map<String, SettableValue> byName;
		final Supplier<String> sqlSupplier;
		final StatementFilterFunction filterFunction;

		ExecuteSpecSupport(Supplier<String> sqlSupplier) {

			this.byIndex = Collections.emptyMap();
			this.byName = Collections.emptyMap();
			this.sqlSupplier = sqlSupplier;
			this.filterFunction = StatementFilterFunctions.empty();
		}

		ExecuteSpecSupport(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {

			this.byIndex = byIndex;
			this.byName = byName;
			this.sqlSupplier = sqlSupplier;
			this.filterFunction = filterFunction;
		}

		<T> FetchSpec<T> exchange(Supplier<String> sqlSupplier, BiFunction<Row, RowMetadata, T> mappingFunction) {

			String sql = getRequiredSql(sqlSupplier);

			Function<Connection, Statement> statementFactory = it -> {

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

					Map<String, SettableValue> remainderByName = new LinkedHashMap<>(this.byName);
					Map<Integer, SettableValue> remainderByIndex = new LinkedHashMap<>(this.byIndex);
					PreparedOperation<?> operation = dataAccessStrategy.processNamedParameters(sql, (index, name) -> {

						if (byName.containsKey(name)) {
							remainderByName.remove(name);
							return dataAccessStrategy.getBindValue(byName.get(name));
						}

						if (byIndex.containsKey(index)) {
							remainderByIndex.remove(index);
							return dataAccessStrategy.getBindValue(byIndex.get(index));
						}

						return null;
					});

					String expanded = getRequiredSql(operation);
					if (logger.isTraceEnabled()) {
						logger.trace("Expanded SQL [" + expanded + "]");
					}

					Statement statement = it.createStatement(expanded);
					BindTarget bindTarget = new StatementWrapper(statement);

					operation.bindTo(bindTarget);

					bindByName(statement, remainderByName);
					bindByIndex(statement, remainderByIndex);

					return statement;
				}

				Statement statement = it.createStatement(sql);

				bindByIndex(statement, this.byIndex);
				bindByName(statement, this.byName);

				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = toFunction(sql, filterFunction, statementFactory);

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> sumRowsUpdated(resultFunction, it), //
					mappingFunction);
		}

		public ExecuteSpecSupport bind(int index, Object value) {

			assertNotPreparedOperation();
			Assert.notNull(value, () -> String.format("Value at index %d must not be null. Use bindNull(…) instead.", index));

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);

			if (value instanceof SettableValue) {
				byIndex.put(index, (SettableValue) value);
			} else {
				byIndex.put(index, SettableValue.fromOrEmpty(value, value.getClass()));
			}

			return createInstance(byIndex, this.byName, this.sqlSupplier, this.filterFunction);
		}

		public ExecuteSpecSupport bindNull(int index, Class<?> type) {

			assertNotPreparedOperation();

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, SettableValue.empty(type));

			return createInstance(byIndex, this.byName, this.sqlSupplier, this.filterFunction);
		}

		public ExecuteSpecSupport bind(String name, Object value) {

			assertNotPreparedOperation();

			Assert.hasText(name, "Parameter name must not be null or empty!");
			Assert.notNull(value,
					() -> String.format("Value for parameter %s must not be null. Use bindNull(…) instead.", name));

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);

			if (value instanceof SettableValue) {
				byName.put(name, (SettableValue) value);
			} else {
				byName.put(name, SettableValue.fromOrEmpty(value, value.getClass()));
			}

			return createInstance(this.byIndex, byName, this.sqlSupplier, this.filterFunction);
		}

		public ExecuteSpecSupport bindNull(String name, Class<?> type) {

			assertNotPreparedOperation();
			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, SettableValue.empty(type));

			return createInstance(this.byIndex, byName, this.sqlSupplier, this.filterFunction);
		}

		public ExecuteSpecSupport filter(StatementFilterFunction filter) {

			Assert.notNull(filter, "Statement FilterFunction must not be null!");

			return createInstance(this.byIndex, byName, this.sqlSupplier, this.filterFunction.andThen(filter));
		}

		private void assertNotPreparedOperation() {
			if (this.sqlSupplier instanceof PreparedOperation<?>) {
				throw new InvalidDataAccessApiUsageException("Cannot add bindings to a PreparedOperation");
			}
		}

		protected ExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {
			return new ExecuteSpecSupport(byIndex, byName, sqlSupplier, filterFunction);
		}
	}

	/**
	 * Default {@link DatabaseClient.GenericExecuteSpec} implementation.
	 */
	protected class DefaultGenericExecuteSpec extends ExecuteSpecSupport implements GenericExecuteSpec {

		DefaultGenericExecuteSpec(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {
			super(byIndex, byName, sqlSupplier, filterFunction);
		}

		DefaultGenericExecuteSpec(Supplier<String> sqlSupplier) {
			super(sqlSupplier);
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return createTypedExecuteSpec(this.byIndex, this.byName, this.sqlSupplier, this.filterFunction, resultType);
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
		public DefaultGenericExecuteSpec filter(StatementFilterFunction filter) {
			return (DefaultGenericExecuteSpec) super.filter(filter);
		}

		@Override
		protected ExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
				Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {
			return createGenericExecuteSpec(byIndex, byName, sqlSupplier, filterFunction);
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
				Supplier<String> sqlSupplier, StatementFilterFunction filterFunction, Class<T> typeToRead) {

			super(byIndex, byName, sqlSupplier, filterFunction);

			this.typeToRead = typeToRead;

			if (typeToRead.isInterface()) {
				this.mappingFunction = ColumnMapRowMapper.INSTANCE
						.andThen(map -> projectionFactory.createProjection(typeToRead, map));
			} else {
				this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
			}
		}

		@Override
		public <R> TypedExecuteSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return createTypedExecuteSpec(this.byIndex, this.byName, this.sqlSupplier, this.filterFunction, resultType);
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
		public DefaultTypedExecuteSpec<T> filter(StatementFilterFunction filter) {
			return (DefaultTypedExecuteSpec<T>) super.filter(filter);
		}

		@Override
		protected DefaultTypedExecuteSpec<T> createInstance(Map<Integer, SettableValue> byIndex,
				Map<String, SettableValue> byName, Supplier<String> sqlSupplier, StatementFilterFunction filterFunction) {
			return createTypedExecuteSpec(byIndex, byName, sqlSupplier, filterFunction, this.typeToRead);
		}
	}

	/**
	 * Default {@link DatabaseClient.SelectFromSpec} implementation.
	 */
	class DefaultSelectFromSpec implements SelectFromSpec {

		@Override
		public GenericSelectSpec from(SqlIdentifier table) {
			return new DefaultGenericSelectSpec(table);
		}

		@Override
		public <T> TypedSelectSpec<T> from(Class<T> table) {

			assertRegularClass(table);

			return new DefaultTypedSelectSpec<>(table);
		}
	}

	/**
	 * Base class for {@link DatabaseClient.GenericExecuteSpec} implementations.
	 */
	private abstract class DefaultSelectSpecSupport {

		final SqlIdentifier table;
		final List<SqlIdentifier> projectedFields;
		final @Nullable CriteriaDefinition criteria;
		final Sort sort;
		final Pageable page;

		DefaultSelectSpecSupport(SqlIdentifier table) {

			Assert.notNull(table, "Table name must not be null!");

			this.table = table;
			this.projectedFields = Collections.emptyList();
			this.criteria = null;
			this.sort = Sort.unsorted();
			this.page = Pageable.unpaged();
		}

		DefaultSelectSpecSupport(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page) {
			this.table = table;
			this.projectedFields = projectedFields;
			this.criteria = criteria;
			this.sort = sort;
			this.page = page;
		}

		public DefaultSelectSpecSupport project(SqlIdentifier... selectedFields) {
			Assert.notNull(selectedFields, "Projection fields must not be null!");

			List<SqlIdentifier> projectedFields = new ArrayList<>(this.projectedFields.size() + selectedFields.length);
			projectedFields.addAll(this.projectedFields);
			projectedFields.addAll(Arrays.asList(selectedFields));

			return createInstance(this.table, projectedFields, this.criteria, this.sort, this.page);
		}

		public DefaultSelectSpecSupport where(CriteriaDefinition whereCriteria) {

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
			Function<Connection, Flux<Result>> resultFunction = toFunction(sql, StatementFilterFunctions.empty(),
					selectFunction);

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> Mono.error(new UnsupportedOperationException("Not available for SELECT")), //
					mappingFunction);
		}

		protected abstract DefaultSelectSpecSupport createInstance(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page);
	}

	private class DefaultGenericSelectSpec extends DefaultSelectSpecSupport implements GenericSelectSpec {

		DefaultGenericSelectSpec(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page) {
			super(table, projectedFields, criteria, sort, page);
		}

		DefaultGenericSelectSpec(SqlIdentifier table) {
			super(table);
		}

		@Override
		public <R> TypedSelectSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			BiFunction<Row, RowMetadata, R> rowMapper;

			if (resultType.isInterface()) {
				rowMapper = ColumnMapRowMapper.INSTANCE.andThen(map -> projectionFactory.createProjection(resultType, map));
			} else {
				rowMapper = dataAccessStrategy.getRowMapper(resultType);
			}

			return new DefaultTypedSelectSpec<>(this.table, this.projectedFields, this.criteria, this.sort, this.page,
					resultType, rowMapper);
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
		public DefaultGenericSelectSpec project(SqlIdentifier... selectedFields) {
			return (DefaultGenericSelectSpec) super.project(selectedFields);
		}

		@Override
		public DefaultGenericSelectSpec matching(CriteriaDefinition criteria) {
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

			StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.table)
					.withProjection(this.projectedFields.toArray(new SqlIdentifier[0])).withSort(this.sort).withPage(this.page);

			if (this.criteria != null) {
				selectSpec = selectSpec.withCriteria(this.criteria);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);
			return execute(operation, mappingFunction);
		}

		@Override
		protected DefaultGenericSelectSpec createInstance(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page) {
			return new DefaultGenericSelectSpec(table, projectedFields, criteria, sort, page);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	@SuppressWarnings("unchecked")
	private class DefaultTypedSelectSpec<T> extends DefaultSelectSpecSupport implements TypedSelectSpec<T> {

		private final Class<T> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedSelectSpec(Class<T> typeToRead) {

			super(dataAccessStrategy.getTableName(typeToRead));

			this.typeToRead = typeToRead;
			this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
		}

		DefaultTypedSelectSpec(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page, Class<T> typeToRead,
				BiFunction<Row, RowMetadata, T> mappingFunction) {

			super(table, projectedFields, criteria, sort, page);

			this.typeToRead = typeToRead;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public <R> FetchSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			BiFunction<Row, RowMetadata, R> rowMapper;

			if (resultType.isInterface()) {
				rowMapper = dataAccessStrategy.getRowMapper(typeToRead)
						.andThen(r -> projectionFactory.createProjection(resultType, r));
			} else {
				rowMapper = dataAccessStrategy.getRowMapper(resultType);
			}

			return exchange(rowMapper);
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
		public DefaultTypedSelectSpec<T> project(SqlIdentifier... selectedFields) {
			return (DefaultTypedSelectSpec<T>) super.project(selectedFields);
		}

		@Override
		public DefaultTypedSelectSpec<T> matching(CriteriaDefinition criteria) {
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

			List<SqlIdentifier> columns;
			StatementMapper mapper = dataAccessStrategy.getStatementMapper().forType(this.typeToRead);

			if (this.projectedFields.isEmpty()) {
				columns = dataAccessStrategy.getAllColumns(this.typeToRead);
			} else {
				columns = this.projectedFields;
			}

			StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.table)
					.withProjection(columns.toArray(new SqlIdentifier[0])).withPage(this.page).withSort(this.sort);

			if (this.criteria != null) {
				selectSpec = selectSpec.withCriteria(this.criteria);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);

			return execute(operation, mappingFunction);
		}

		@Override
		protected DefaultTypedSelectSpec<T> createInstance(SqlIdentifier table, List<SqlIdentifier> projectedFields,
				@Nullable CriteriaDefinition criteria, Sort sort, Pageable page) {
			return new DefaultTypedSelectSpec<>(table, projectedFields, criteria, sort, page, this.typeToRead,
					this.mappingFunction);
		}
	}

	/**
	 * Default {@link DatabaseClient.InsertIntoSpec} implementation.
	 */
	class DefaultInsertIntoSpec implements InsertIntoSpec {

		@Override
		public GenericInsertSpec<Map<String, Object>> into(SqlIdentifier table) {
			return new DefaultGenericInsertSpec<>(table, Collections.emptyMap(), ColumnMapRowMapper.INSTANCE);
		}

		@Override
		public <T> TypedInsertSpec<T> into(Class<T> table) {

			assertRegularClass(table);

			return new DefaultTypedInsertSpec<>(table, ColumnMapRowMapper.INSTANCE);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.GenericInsertSpec}.
	 */
	class DefaultGenericInsertSpec<T> implements GenericInsertSpec<T> {

		private final SqlIdentifier table;
		private final Map<SqlIdentifier, SettableValue> byName;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultGenericInsertSpec(SqlIdentifier table, Map<SqlIdentifier, SettableValue> byName,
				BiFunction<Row, RowMetadata, T> mappingFunction) {
			this.table = table;
			this.byName = byName;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public GenericInsertSpec<T> value(SqlIdentifier field, Object value) {

			Assert.notNull(field, "Field must not be null!");

			Map<SqlIdentifier, SettableValue> byName = new LinkedHashMap<>(this.byName);

			if (value instanceof SettableValue) {
				byName.put(field, (SettableValue) value);
			} else {
				byName.put(field, SettableValue.fromOrEmpty(value, value.getClass()));
			}

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

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();
			StatementMapper.InsertSpec insert = mapper.createInsert(this.table);

			for (SqlIdentifier column : this.byName.keySet()) {
				insert = insert.withColumn(column, this.byName.get(column));
			}

			PreparedOperation<?> operation = mapper.getMappedObject(insert);
			return exchangeInsert(mappingFunction, operation);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	class DefaultTypedInsertSpec<T, R> implements TypedInsertSpec<T>, InsertSpec<R> {

		private final Class<?> typeToInsert;
		private final SqlIdentifier table;
		private final Publisher<T> objectToInsert;
		private final BiFunction<Row, RowMetadata, R> mappingFunction;

		DefaultTypedInsertSpec(Class<?> typeToInsert, BiFunction<Row, RowMetadata, R> mappingFunction) {

			this.typeToInsert = typeToInsert;
			this.table = dataAccessStrategy.getTableName(typeToInsert);
			this.objectToInsert = Mono.empty();
			this.mappingFunction = mappingFunction;
		}

		DefaultTypedInsertSpec(Class<?> typeToInsert, SqlIdentifier table, Publisher<T> objectToInsert,
				BiFunction<Row, RowMetadata, R> mappingFunction) {
			this.typeToInsert = typeToInsert;
			this.table = table;
			this.objectToInsert = objectToInsert;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public TypedInsertSpec<T> table(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null!");

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

			for (SqlIdentifier column : outboundRow.keySet()) {
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
		public GenericUpdateSpec table(SqlIdentifier table) {
			return new DefaultGenericUpdateSpec(null, table, null, null);
		}

		@Override
		public <T> TypedUpdateSpec<T> table(Class<T> table) {

			assertRegularClass(table);

			return new DefaultTypedUpdateSpec<>(table, null, null, null);
		}
	}

	class DefaultGenericUpdateSpec implements GenericUpdateSpec, UpdateMatchingSpec {

		private final @Nullable Class<?> typeToUpdate;
		private final @Nullable SqlIdentifier table;
		private final @Nullable org.springframework.data.relational.core.query.Update assignments;
		private final @Nullable CriteriaDefinition where;

		DefaultGenericUpdateSpec(@Nullable Class<?> typeToUpdate, @Nullable SqlIdentifier table,
				@Nullable org.springframework.data.relational.core.query.Update assignments,
				@Nullable CriteriaDefinition where) {
			this.typeToUpdate = typeToUpdate;
			this.table = table;
			this.assignments = assignments;
			this.where = where;
		}

		@Override
		public UpdateMatchingSpec using(Update update) {

			Assert.notNull(update, "Update must not be null");

			return new DefaultGenericUpdateSpec(this.typeToUpdate, this.table,
					org.springframework.data.relational.core.query.Update.from(update.getAssignments()), this.where);
		}

		@Override
		public UpdateMatchingSpec using(org.springframework.data.relational.core.query.Update update) {

			Assert.notNull(update, "Update must not be null");

			return new DefaultGenericUpdateSpec(this.typeToUpdate, this.table, update, this.where);
		}

		@Override
		public UpdateSpec matching(CriteriaDefinition criteria) {

			Assert.notNull(criteria, "Criteria must not be null");

			return new DefaultGenericUpdateSpec(this.typeToUpdate, this.table, this.assignments, criteria);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			SqlIdentifier table;

			if (StringUtils.isEmpty(this.table)) {

				Assert.state(this.typeToUpdate != null, "Type to update must not be null!");

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

		private UpdatedRowsFetchSpec exchange(SqlIdentifier table) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();

			if (this.typeToUpdate != null) {
				mapper = mapper.forType(this.typeToUpdate);
			}

			Assert.state(this.assignments != null, "Update assignments must not be null!");

			StatementMapper.UpdateSpec update = mapper.createUpdate(table, this.assignments);

			if (this.where != null) {
				update = update.withCriteria(this.where);
			}

			PreparedOperation<?> operation = mapper.getMappedObject(update);

			return exchangeUpdate(operation);
		}
	}

	class DefaultTypedUpdateSpec<T> implements TypedUpdateSpec<T>, UpdateMatchingSpec {

		private final Class<T> typeToUpdate;
		private final @Nullable SqlIdentifier table;
		private final @Nullable T objectToUpdate;
		private final @Nullable CriteriaDefinition where;

		DefaultTypedUpdateSpec(Class<T> typeToUpdate, @Nullable SqlIdentifier table, @Nullable T objectToUpdate,
				@Nullable CriteriaDefinition where) {

			this.typeToUpdate = typeToUpdate;
			this.table = table;
			this.objectToUpdate = objectToUpdate;
			this.where = where;
		}

		@Override
		public UpdateMatchingSpec using(T objectToUpdate) {

			Assert.notNull(objectToUpdate, "Object to update must not be null");

			return new DefaultTypedUpdateSpec<>(this.typeToUpdate, this.table, objectToUpdate, this.where);
		}

		@Override
		public TypedUpdateSpec<T> table(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null!");

			return new DefaultTypedUpdateSpec<>(this.typeToUpdate, tableName, this.objectToUpdate, this.where);
		}

		@Override
		public UpdateSpec matching(CriteriaDefinition criteria) {

			Assert.notNull(criteria, "Criteria must not be null!");

			return new DefaultTypedUpdateSpec<>(this.typeToUpdate, this.table, this.objectToUpdate, criteria);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			SqlIdentifier table;

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

		private UpdatedRowsFetchSpec exchange(SqlIdentifier table) {

			StatementMapper mapper = dataAccessStrategy.getStatementMapper();
			Map<SqlIdentifier, SettableValue> columns = dataAccessStrategy.getOutboundRow(this.objectToUpdate);
			List<SqlIdentifier> ids = dataAccessStrategy.getIdentifierColumns(this.typeToUpdate);

			if (ids.isEmpty()) {
				throw new IllegalStateException("No identifier columns in " + this.typeToUpdate.getName() + "!");
			}
			Object id = columns.remove(ids.get(0)); // do not update the Id column.

			org.springframework.data.relational.core.query.Update update = null;

			for (SqlIdentifier column : columns.keySet()) {
				if (update == null) {
					update = org.springframework.data.relational.core.query.Update.update(dataAccessStrategy.toSql(column),
							columns.get(column));
				} else {
					update = update.set(dataAccessStrategy.toSql(column), columns.get(column));
				}
			}

			Criteria updateCriteria = org.springframework.data.relational.core.query.Criteria
					.where(dataAccessStrategy.toSql(ids.get(0))).is(id);
			if (this.where != null) {
				updateCriteria = updateCriteria.and(this.where);
			}

			PreparedOperation<?> operation = mapper
					.getMappedObject(mapper.createUpdate(table, update).withCriteria(updateCriteria));

			return exchangeUpdate(operation);
		}
	}

	/**
	 * Default {@link DatabaseClient.DeleteFromSpec} implementation.
	 */
	class DefaultDeleteFromSpec implements DeleteFromSpec {

		@Override
		public DefaultDeleteSpec<?> from(SqlIdentifier table) {
			return new DefaultDeleteSpec<>(null, table, null);
		}

		@Override
		public <T> DefaultDeleteSpec<T> from(Class<T> table) {

			assertRegularClass(table);

			return new DefaultDeleteSpec<>(table, null, null);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	class DefaultDeleteSpec<T> implements DeleteMatchingSpec, TypedDeleteSpec<T> {

		private final @Nullable Class<T> typeToDelete;
		private final @Nullable SqlIdentifier table;
		private final @Nullable CriteriaDefinition where;

		DefaultDeleteSpec(@Nullable Class<T> typeToDelete, @Nullable SqlIdentifier table,
				@Nullable CriteriaDefinition where) {
			this.typeToDelete = typeToDelete;
			this.table = table;
			this.where = where;
		}

		@Override
		public DeleteSpec matching(CriteriaDefinition criteria) {

			Assert.notNull(criteria, "Criteria must not be null!");

			return new DefaultDeleteSpec<>(this.typeToDelete, this.table, criteria);
		}

		@Override
		public TypedDeleteSpec<T> table(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null!");

			return new DefaultDeleteSpec<>(this.typeToDelete, tableName, this.where);
		}

		@Override
		public UpdatedRowsFetchSpec fetch() {

			SqlIdentifier table;

			if (StringUtils.isEmpty(this.table)) {

				Assert.state(this.typeToDelete != null, "Type to delete must not be null!");

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

		private UpdatedRowsFetchSpec exchange(SqlIdentifier table) {

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
		Function<Connection, Flux<Result>> resultFunction = toFunction(sql, StatementFilterFunctions.empty(),
				insertFunction);

		return new DefaultSqlResult<>(this, //
				sql, //
				resultFunction, //
				it -> sumRowsUpdated(resultFunction, it), //
				mappingFunction);
	}

	private UpdatedRowsFetchSpec exchangeUpdate(PreparedOperation<?> operation) {

		String sql = getRequiredSql(operation);
		Function<Connection, Statement> executeFunction = wrapPreparedOperation(sql, operation);
		Function<Connection, Flux<Result>> resultFunction = toFunction(sql, StatementFilterFunctions.empty(),
				executeFunction);

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

	private Function<Connection, Flux<Result>> toFunction(String sql, StatementFilterFunction filterFunction,
			Function<Connection, Statement> statementFactory) {

		return it -> {

			Flux<Result> from = Flux.defer(() -> {

				Statement statement = statementFactory.apply(it);
				return filterFunction.filter(statement, executeFunction);
			}).cast(Result.class);
			return from.checkpoint("SQL \"" + sql + "\" [DatabaseClient]");
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

	private static void assertRegularClass(Class<?> table) {

		Assert.notNull(table, "Entity type must not be null");
		Assert.isTrue(!table.isInterface() && !table.isEnum(),
				() -> String.format("Entity type %s must be a class", table.getName()));
	}

	/**
	 * Invocation handler that suppresses close calls on R2DBC Connections. Also prepares returned Statement
	 * (Prepared/CallbackStatement) objects.
	 *
	 * @see Connection#close()
	 */
	private static class CloseSuppressingInvocationHandler implements InvocationHandler {

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
				return target;
			} else if (method.getName().equals("close")) {
				// Handle close method: suppress, not valid.
				return Mono.error(new UnsupportedOperationException("Close is not supported!"));
			} else if (method.getName().equals("getTargetConnection")) {
				// Handle getTargetConnection method: return underlying Connection.
				return this.target;
			}

			// Invoke method on target Connection.
			try {
				return method.invoke(this.target, args);
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

	/**
	 * Holder for a connection that makes sure the close action is invoked atomically only once.
	 */
	static class ConnectionCloseHolder extends AtomicBoolean {

		private static final long serialVersionUID = -8994138383301201380L;

		final Connection connection;
		final Function<Connection, Publisher<Void>> closeFunction;

		ConnectionCloseHolder(Connection connection, Function<Connection, Publisher<Void>> closeFunction) {
			this.connection = connection;
			this.closeFunction = closeFunction;
		}

		Mono<Void> close() {

			return Mono.defer(() -> {

				if (compareAndSet(false, true)) {
					return Mono.from(this.closeFunction.apply(this.connection));
				}

				return Mono.empty();
			});
		}
	}

	static class StatementWrapper implements BindTarget {

		final Statement statement;

		StatementWrapper(Statement statement) {
			this.statement = statement;
		}

		@Override
		public void bind(String identifier, Object value) {
			this.statement.bind(identifier, value);
		}

		@Override
		public void bind(int index, Object value) {
			this.statement.bind(index, value);
		}

		@Override
		public void bindNull(String identifier, Class<?> type) {
			this.statement.bindNull(identifier, type);
		}

		@Override
		public void bindNull(int index, Class<?> type) {
			this.statement.bindNull(index, type);
		}
	}

}
