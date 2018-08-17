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
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.NullHandling;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy.SettableValue;
import org.springframework.data.r2dbc.function.connectionfactory.ConnectionProxy;
import org.springframework.data.r2dbc.function.convert.ColumnMapRowMapper;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
class DefaultDatabaseClient implements DatabaseClient, ConnectionAccessor {

	/** Logger available to subclasses */
	private final Log logger = LogFactory.getLog(getClass());

	private final ConnectionFactory connector;

	private final R2dbcExceptionTranslator exceptionTranslator;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final DefaultDatabaseClientBuilder builder;

	DefaultDatabaseClient(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
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
		return new DefaultSelectFromSpec();
	}

	@Override
	public InsertIntoSpec insert() {
		return new DefaultInsertIntoSpec();
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
	@Override
	public <T> Mono<T> inConnection(Function<Connection, Mono<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<Connection> connectionMono = getConnection();
		// Create close-suppressing Connection proxy, also preparing returned Statements.

		return Mono.usingWhen(connectionMono, it -> {

			Connection connectionToUse = createConnectionProxy(it);

			return doInConnection(connectionToUse, action);
		}, this::closeConnection, this::closeConnection, this::closeConnection) //
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
	 * @throws DataAccessException
	 */
	@Override
	public <T> Flux<T> inConnectionMany(Function<Connection, Flux<T>> action) throws DataAccessException {

		Assert.notNull(action, "Callback object must not be null");

		Mono<Connection> connectionMono = getConnection();
		// Create close-suppressing Connection proxy, also preparing returned Statements.

		return Flux.usingWhen(connectionMono, it -> {

			Connection connectionToUse = createConnectionProxy(it);

			return doInConnectionMany(connectionToUse, action);
		}, this::closeConnection, this::closeConnection, this::closeConnection) //
				.onErrorMap(R2dbcException.class, ex -> translateException("executeMany", getSql(action), ex));
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
	 * @return the ConnectionFactory (never {@literal null})
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
	 * Translate the given {@link R2dbcException} into a generic {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted.
	 * @param sql SQL query or update that caused the problem (may be {@literal null}).
	 * @param ex the offending {@link R2dbcException}.
	 * @return a DataAccessException wrapping the {@link R2dbcException} (never {@literal null}).
	 */
	protected DataAccessException translateException(String task, @Nullable String sql, R2dbcException ex) {

		DataAccessException dae = exceptionTranslator.translate(task, sql, ex);
		return (dae != null ? dae : new UncategorizedR2dbcException(task, sql, ex));
	}

	private static void doBind(Statement statement, Map<String, SettableValue> byName,
			Map<Integer, SettableValue> byIndex) {

		byIndex.forEach((i, o) -> {

			if (o.getValue() != null) {
				statement.bind(i, o.getValue());
			} else {
				statement.bindNull(i, o.getType());
			}
		});

		byName.forEach((name, o) -> {

			if (o.getValue() != null) {
				statement.bind(name, o.getValue());
			} else {
				statement.bindNull(name, o.getType());
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

			return new DefaultGenericExecuteSpec(sqlSupplier);
		}
	}

	/**
	 * Base class for {@link DatabaseClient.GenericExecuteSpec} implementations.
	 */
	@RequiredArgsConstructor
	private class GenericExecuteSpecSupport {

		final Map<Integer, SettableValue> byIndex;
		final Map<String, SettableValue> byName;
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

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}

		public GenericExecuteSpecSupport bind(int index, Object value) {

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, new SettableValue(index, value, null));

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bindNull(int index, Class<?> type) {

			Map<Integer, SettableValue> byIndex = new LinkedHashMap<>(this.byIndex);
			byIndex.put(index, new SettableValue(index, null, type));

			return createInstance(byIndex, this.byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bind(String name, Object value) {

			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, new SettableValue(name, value, null));

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		public GenericExecuteSpecSupport bindNull(String name, Class<?> type) {

			Assert.hasText(name, "Parameter name must not be null or empty!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(name, new SettableValue(name, null, type));

			return createInstance(this.byIndex, byName, this.sqlSupplier);
		}

		protected GenericExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex,
				Map<String, SettableValue> byName, Supplier<String> sqlSupplier) {
			return new GenericExecuteSpecSupport(byIndex, byName, sqlSupplier);
		}

		public GenericExecuteSpecSupport bind(Object bean) {

			Assert.notNull(bean, "Bean must not be null!");

			throw new UnsupportedOperationException("Implement me!");
		}
	}

	/**
	 * Default {@link DatabaseClient.GenericExecuteSpec} implementation.
	 */
	private class DefaultGenericExecuteSpec extends GenericExecuteSpecSupport implements GenericExecuteSpec {

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
		protected GenericExecuteSpecSupport createInstance(Map<Integer, SettableValue> byIndex,
				Map<String, SettableValue> byName, Supplier<String> sqlSupplier) {
			return new DefaultGenericExecuteSpec(byIndex, byName, sqlSupplier);
		}
	}

	/**
	 * Default {@link DatabaseClient.GenericExecuteSpec} implementation.
	 */
	@SuppressWarnings("unchecked")
	private class DefaultTypedGenericExecuteSpec<T> extends GenericExecuteSpecSupport implements TypedExecuteSpec<T> {

		private final Class<T> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedGenericExecuteSpec(Map<Integer, SettableValue> byIndex, Map<String, SettableValue> byName,
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
		public DefaultTypedGenericExecuteSpec<T> bindNull(int index, Class<?> type) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bindNull(index, type);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bind(String name, Object value) {
			return (DefaultTypedGenericExecuteSpec) super.bind(name, value);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bindNull(String name, Class<?> type) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bindNull(name, type);
		}

		@Override
		public DefaultTypedGenericExecuteSpec<T> bind(Object bean) {
			return (DefaultTypedGenericExecuteSpec<T>) super.bind(bean);
		}

		@Override
		protected DefaultTypedGenericExecuteSpec<T> createInstance(Map<Integer, SettableValue> byIndex,
				Map<String, SettableValue> byName, Supplier<String> sqlSupplier) {
			return new DefaultTypedGenericExecuteSpec<>(byIndex, byName, sqlSupplier, typeToRead);
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
		final Sort sort;
		final Pageable page;

		DefaultSelectSpecSupport(String table) {

			Assert.hasText(table, "Table name must not be null!");

			this.table = table;
			this.projectedFields = Collections.emptyList();
			this.sort = Sort.unsorted();
			this.page = Pageable.unpaged();
		}

		public DefaultSelectSpecSupport project(String... selectedFields) {
			Assert.notNull(selectedFields, "Projection fields must not be null!");

			List<String> projectedFields = new ArrayList<>(this.projectedFields.size() + selectedFields.length);
			projectedFields.addAll(this.projectedFields);
			projectedFields.addAll(Arrays.asList(selectedFields));

			return createInstance(table, projectedFields, sort, page);
		}

		public DefaultSelectSpecSupport orderBy(Sort sort) {

			Assert.notNull(sort, "Sort must not be null!");

			return createInstance(table, projectedFields, sort, page);
		}

		public DefaultSelectSpecSupport page(Pageable page) {

			Assert.notNull(page, "Pageable must not be null!");

			return createInstance(table, projectedFields, sort, page);
		}

		StringBuilder getLimitOffset(Pageable pageable) {
			return new StringBuilder().append("LIMIT").append(' ').append(page.getPageSize()) //
					.append(' ').append("OFFSET").append(' ').append(page.getOffset());
		}

		StringBuilder getSortClause(Sort sort) {

			StringBuilder sortClause = new StringBuilder();

			for (Order order : sort) {

				if (sortClause.length() != 0) {
					sortClause.append(',').append(' ');
				}

				sortClause.append(order.getProperty()).append(' ').append(order.getDirection().isAscending() ? "ASC" : "DESC");

				if (order.getNullHandling() == NullHandling.NULLS_FIRST) {
					sortClause.append(' ').append("NULLS FIRST");
				}

				if (order.getNullHandling() == NullHandling.NULLS_LAST) {
					sortClause.append(' ').append("NULLS LAST");
				}
			}
			return sortClause;
		}

		<R> SqlResult<R> execute(String sql, BiFunction<Row, RowMetadata, R> mappingFunction) {

			Function<Connection, Statement> selectFunction = it -> {

				if (logger.isDebugEnabled()) {
					logger.debug("Executing SQL statement [" + sql + "]");
				}

				return it.createStatement(sql);
			};

			Function<Connection, Flux<Result>> resultFunction = it -> Flux.from(selectFunction.apply(it).execute());

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> Mono.error(new UnsupportedOperationException("Not available for SELECT")), //
					mappingFunction);
		}

		protected abstract DefaultSelectSpecSupport createInstance(String table, List<String> projectedFields, Sort sort,
				Pageable page);
	}

	private class DefaultGenericSelectSpec extends DefaultSelectSpecSupport implements GenericSelectSpec {

		public DefaultGenericSelectSpec(String table, List<String> projectedFields, Sort sort, Pageable page) {
			super(table, projectedFields, sort, page);
		}

		DefaultGenericSelectSpec(String table) {
			super(table);
		}

		@Override
		public <R> TypedSelectSpec<R> as(Class<R> resultType) {
			return new DefaultTypedSelectSpec<>(table, projectedFields, sort, page, resultType,
					dataAccessStrategy.getRowMapper(resultType));
		}

		@Override
		public DefaultGenericSelectSpec project(String... selectedFields) {
			return (DefaultGenericSelectSpec) super.project(selectedFields);
		}

		@Override
		public DefaultGenericSelectSpec orderBy(Sort sort) {
			return (DefaultGenericSelectSpec) super.orderBy(sort);
		}

		@Override
		public DefaultGenericSelectSpec page(Pageable page) {
			return (DefaultGenericSelectSpec) super.page(page);
		}

		@Override
		public FetchSpec<Map<String, Object>> fetch() {
			return exchange(ColumnMapRowMapper.INSTANCE);
		}

		@Override
		public Mono<SqlResult<Map<String, Object>>> exchange() {
			return Mono.just(exchange(ColumnMapRowMapper.INSTANCE));
		}

		private <R> SqlResult<R> exchange(BiFunction<Row, RowMetadata, R> mappingFunction) {

			List<String> projectedFields;

			if (this.projectedFields.isEmpty()) {
				projectedFields = Collections.singletonList("*");
			} else {
				projectedFields = this.projectedFields;
			}

			StringBuilder selectBuilder = new StringBuilder();
			selectBuilder.append("SELECT").append(' ') //
					.append(StringUtils.collectionToDelimitedString(projectedFields, ", ")).append(' ') //
					.append("FROM").append(' ').append(table);

			if (sort.isSorted()) {
				selectBuilder.append(' ').append("ORDER BY").append(' ').append(getSortClause(sort));
			}

			if (page.isPaged()) {
				selectBuilder.append(' ').append(getLimitOffset(page));
			}

			return execute(selectBuilder.toString(), mappingFunction);
		}

		@Override
		protected DefaultGenericSelectSpec createInstance(String table, List<String> projectedFields, Sort sort,
				Pageable page) {
			return new DefaultGenericSelectSpec(table, projectedFields, sort, page);
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
	 */
	@SuppressWarnings("unchecked")
	private class DefaultTypedSelectSpec<T> extends DefaultSelectSpecSupport implements TypedSelectSpec<T> {

		private final Class<?> typeToRead;
		private final BiFunction<Row, RowMetadata, T> mappingFunction;

		DefaultTypedSelectSpec(Class<T> typeToRead) {

			super(dataAccessStrategy.getTableName(typeToRead));

			this.typeToRead = typeToRead;
			this.mappingFunction = dataAccessStrategy.getRowMapper(typeToRead);
		}

		DefaultTypedSelectSpec(String table, List<String> projectedFields, Sort sort, Pageable page, Class<?> typeToRead,
				BiFunction<Row, RowMetadata, T> mappingFunction) {
			super(table, projectedFields, sort, page);
			this.typeToRead = typeToRead;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public <R> TypedSelectSpec<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "Result type must not be null!");

			return new DefaultTypedSelectSpec<>(table, projectedFields, sort, page, typeToRead,
					dataAccessStrategy.getRowMapper(resultType));
		}

		@Override
		public <R> TypedSelectSpec<R> extract(BiFunction<Row, RowMetadata, R> mappingFunction) {

			Assert.notNull(mappingFunction, "Mapping function must not be null!");

			return new DefaultTypedSelectSpec<>(table, projectedFields, sort, page, typeToRead, mappingFunction);
		}

		@Override
		public DefaultTypedSelectSpec<T> project(String... selectedFields) {
			return (DefaultTypedSelectSpec<T>) super.project(selectedFields);
		}

		@Override
		public DefaultTypedSelectSpec<T> orderBy(Sort sort) {
			return (DefaultTypedSelectSpec<T>) super.orderBy(sort);
		}

		@Override
		public DefaultTypedSelectSpec<T> page(Pageable page) {
			return (DefaultTypedSelectSpec<T>) super.page(page);
		}

		@Override
		public FetchSpec<T> fetch() {
			return exchange(mappingFunction);
		}

		@Override
		public Mono<SqlResult<T>> exchange() {
			return Mono.just(exchange(mappingFunction));
		}

		private <R> SqlResult<R> exchange(BiFunction<Row, RowMetadata, R> mappingFunction) {

			List<String> projectedFields;

			if (this.projectedFields.isEmpty()) {
				projectedFields = dataAccessStrategy.getAllFields(typeToRead);
			} else {
				projectedFields = this.projectedFields;
			}

			StringBuilder selectBuilder = new StringBuilder();
			selectBuilder.append("SELECT").append(' ') //
					.append(StringUtils.collectionToDelimitedString(projectedFields, ", ")).append(' ') //
					.append("FROM").append(' ').append(table);

			if (sort.isSorted()) {

				Sort mappedSort = dataAccessStrategy.getMappedSort(typeToRead, sort);
				selectBuilder.append(' ').append("ORDER BY").append(' ').append(getSortClause(mappedSort));
			}

			if (page.isPaged()) {
				selectBuilder.append(' ').append(getLimitOffset(page));
			}

			return execute(selectBuilder.toString(), mappingFunction);
		}

		@Override
		protected DefaultTypedSelectSpec<T> createInstance(String table, List<String> projectedFields, Sort sort,
				Pageable page) {
			return new DefaultTypedSelectSpec<>(table, projectedFields, sort, page, typeToRead, mappingFunction);
		}
	}

	/**
	 * Default {@link DatabaseClient.InsertIntoSpec} implementation.
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
	 * Default implementation of {@link DatabaseClient.GenericInsertSpec}.
	 */
	@RequiredArgsConstructor
	class DefaultGenericInsertSpec implements GenericInsertSpec {

		private final String table;
		private final Map<String, SettableValue> byName;

		@Override
		public GenericInsertSpec value(String field, Object value) {

			Assert.notNull(field, "Field must not be null!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(field, new SettableValue(field, value, null));

			return new DefaultGenericInsertSpec(this.table, byName);
		}

		@Override
		public GenericInsertSpec nullValue(String field, Class<?> type) {

			Assert.notNull(field, "Field must not be null!");

			Map<String, SettableValue> byName = new LinkedHashMap<>(this.byName);
			byName.put(field, new SettableValue(field, null, type));

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

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
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

			for (SettableValue value : byName.values()) {

				if (value.getValue() != null) {
					statement.bind(index.getAndIncrement(), value.getValue());
				} else {
					statement.bindNull("$" + (index.getAndIncrement() + 1), value.getType());
				}
			}
		}
	}

	/**
	 * Default implementation of {@link DatabaseClient.TypedInsertSpec}.
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
			return Mono.from(objectToInsert).flatMapMany(toInsert -> exchange(toInsert, (row, md) -> row).all()).then();
		}

		@Override
		public Mono<SqlResult<Map<String, Object>>> exchange() {
			return Mono.from(objectToInsert).map(toInsert -> exchange(toInsert, ColumnMapRowMapper.INSTANCE));
		}

		private <R> SqlResult<R> exchange(Object toInsert, BiFunction<Row, RowMetadata, R> mappingFunction) {

			StringBuilder builder = new StringBuilder();

			List<SettableValue> insertValues = dataAccessStrategy.getInsert(toInsert);
			String fieldNames = insertValues.stream().map(SettableValue::getIdentifier).map(Object::toString)
					.collect(Collectors.joining(","));
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

				for (SettableValue settable : insertValues) {

					if (settable.getValue() != null) {
						statement.bind(index.getAndIncrement(), settable.getValue());
					} else {
						statement.bindNull("$" + (index.getAndIncrement() + 1), settable.getType());
					}
				}

				return statement;
			};

			Function<Connection, Flux<Result>> resultFunction = it -> {
				return Flux.from(insertFunction.apply(it).executeReturningGeneratedKeys());
			};

			return new DefaultSqlResult<>(DefaultDatabaseClient.this, //
					sql, //
					resultFunction, //
					it -> resultFunction.apply(it).flatMap(Result::getRowsUpdated).next(), //
					mappingFunction);
		}
	}

	private static <T> Flux<T> doInConnectionMany(Connection connection, Function<Connection, Flux<T>> action) {

		try {
			return action.apply(connection);
		} catch (R2dbcException e) {

			String sql = getSql(action);
			return Flux.error(new UncategorizedR2dbcException("doInConnectionMany", sql, e) {});
		}
	}

	private static <T> Mono<T> doInConnection(Connection connection, Function<Connection, Mono<T>> action) {

		try {
			return action.apply(connection);
		} catch (R2dbcException e) {

			String sql = getSql(action);
			return Mono.error(new UncategorizedR2dbcException("doInConnection", sql, e) {});
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
}
