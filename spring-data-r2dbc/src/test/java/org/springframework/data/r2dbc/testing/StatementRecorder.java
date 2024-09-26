/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.testing;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.reactivestreams.Publisher;

import org.springframework.r2dbc.core.Parameter;

/**
 * Recorder utility for R2DBC {@link Statement}s. Allows stubbing and introspection.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
public class StatementRecorder implements ConnectionFactory {

	private final Map<Predicate<String>, Supplier<List<Result>>> stubbings = new LinkedHashMap<>();
	private final List<RecordedStatement> createdStatements = new ArrayList<>();
	private final List<RecordedStatement> executedStatements = new ArrayList<>();

	private StatementRecorder() {}

	/**
	 * Create a new {@link StatementRecorder}.
	 *
	 * @return
	 */
	public static StatementRecorder newInstance() {
		return new StatementRecorder();
	}

	/**
	 * Create a new {@link StatementRecorder} accepting a {@link Consumer configurer}.
	 *
	 * @param configurer
	 * @return
	 */
	public static StatementRecorder newInstance(Consumer<StatementRecorder> configurer) {

		StatementRecorder statementRecorder = new StatementRecorder();

		configurer.accept(statementRecorder);

		return statementRecorder;
	}

	/**
	 * Add a stubbing rule given the {@link Predicate SQL Predicate} and a {@link Result} that is emitted by the executed
	 * statement. Typical usage:
	 *
	 * <pre class="code">
	 * recorder.addStubbing(sql -> sql.startsWith("SELECT"), result);
	 * </pre>
	 *
	 * @param sqlPredicate
	 * @param result
	 */
	public void addStubbing(Predicate<String> sqlPredicate, Result result) {
		this.stubbings.put(sqlPredicate, () -> Collections.singletonList(result));
	}

	/**
	 * Add a stubbing rule given the {@link Predicate SQL Predicate} and a list of {@link Result results} that are emitted
	 * by the executed statement. Typical usage:
	 *
	 * <pre class="code">
	 * recorder.addStubbing(sql -> sql.startsWith("SELECT"), results);
	 * </pre>
	 *
	 * @param sqlPredicate
	 * @param result
	 */
	public void addStubbing(Predicate<String> sqlPredicate, List<Result> results) {
		this.stubbings.put(sqlPredicate, () -> results);
	}

	/**
	 * Retrieve a statement by {@code sql}.
	 *
	 * @param sql
	 * @return
	 */
	public RecordedStatement getCreatedStatement(String sql) {
		return getCreatedStatement(it -> compareSql(sql, it));
	}

	private static boolean compareSql(String pattern, String actual) {
		return actual.equals(pattern) || Pattern.compile(pattern).matcher(actual).find();
	}

	/**
	 * Retrieve a statement by a {@link Predicate SQL predicate}.
	 *
	 * @param sql
	 * @return
	 */
	public RecordedStatement getCreatedStatement(Predicate<String> predicate) {

		return createdStatements.stream().filter(recordedStatement -> {
			return predicate.test(recordedStatement.getSql()) || predicate.test(recordedStatement.getSql().toLowerCase())
					|| predicate.test(recordedStatement.getSql().toUpperCase());
		}).findFirst().orElseThrow(() -> new NoSuchElementException("No statement found"));
	}

	public List<RecordedStatement> getCreatedStatements() {
		return createdStatements;
	}

	public List<RecordedStatement> getExecutedStatements() {
		return executedStatements;
	}

	@Override
	public Publisher<? extends Connection> create() {
		return Mono.just(new RecorderConnection());
	}

	@Override
	public ConnectionFactoryMetadata getMetadata() {
		return () -> "StatementRecorder";
	}

	class RecorderConnection implements Connection {

		@Override
		public Publisher<Void> beginTransaction() {
			return createStatement("BEGIN").execute().then();
		}

		@Override
		public Publisher<Void> beginTransaction(TransactionDefinition definition) {
			return createStatement("BEGIN " + definition).execute().then();
		}

		@Override
		public Publisher<Void> close() {
			return createStatement("CLOSE").execute().then();
		}

		@Override
		public Publisher<Void> commitTransaction() {
			return createStatement("COMMIT").execute().then();
		}

		@Override
		public Batch createBatch() {
			throw new UnsupportedOperationException("createBatch not yet supported");
		}

		@Override
		public Publisher<Void> createSavepoint(String name) {
			return createStatement("CREATE SAVEPOINT " + name).execute().then();
		}

		@Override
		public RecordedStatement createStatement(String sql) {

			RecordedStatement statement = doCreateStatement(sql);

			createdStatements.add(statement);

			return statement;
		}

		private RecordedStatement doCreateStatement(String sql) {
			for (Map.Entry<Predicate<String>, Supplier<List<Result>>> entry : stubbings.entrySet()) {

				if (entry.getKey().test(sql) || entry.getKey().test(sql.toLowerCase())
						|| entry.getKey().test(sql.toUpperCase())) {
					return new RecordedStatement(sql, entry.getValue().get());
				}

			}
			return new RecordedStatement(sql, Collections.emptyList());
		}

		@Override
		public boolean isAutoCommit() {
			throw new UnsupportedOperationException("isAutoCommit not yet supported");
		}

		@Override
		public ConnectionMetadata getMetadata() {
			throw new UnsupportedOperationException("getMetadata not yet supported");
		}

		@Override
		public IsolationLevel getTransactionIsolationLevel() {
			throw new UnsupportedOperationException("getTransactionIsolationLevel not yet supported");
		}

		@Override
		public Publisher<Void> releaseSavepoint(String name) {
			return createStatement("RELEASE SAVEPOINT " + name).execute().then();
		}

		@Override
		public Publisher<Void> rollbackTransaction() {
			return createStatement("ROLLBACK").execute().then();
		}

		@Override
		public Publisher<Void> rollbackTransactionToSavepoint(String name) {
			return createStatement("ROLLBACK TO " + name).execute().then();
		}

		@Override
		public Publisher<Void> setAutoCommit(boolean autoCommit) {
			return createStatement("SET AUTOCOMMIT " + autoCommit).execute().then();
		}

		@Override
		public Publisher<Void> setLockWaitTimeout(Duration timeout) {
			return createStatement("SET LOCK WAIT TIMEOUT " + timeout).execute().then();
		}

		@Override
		public Publisher<Void> setStatementTimeout(Duration timeout) {
			return createStatement("SET STATEMENT TIMEOUT " + timeout).execute().then();
		}

		@Override
		public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
			return createStatement("SET TRANSACTION ISOLATION LEVEL " + isolationLevel.asSql()).execute().then();
		}

		@Override
		public Publisher<Boolean> validate(ValidationDepth depth) {
			return Mono.just(true);
		}
	}

	public class RecordedStatement implements Statement {

		private final String sql;

		private final List<Result> results;

		private int fetchSize;

		private final Map<Object, Parameter> bindings = new LinkedHashMap<>();

		public RecordedStatement(String sql, Result result) {
			this(sql, Collections.singletonList(result));
		}

		public RecordedStatement(String sql, List<Result> results) {
			this.sql = sql;
			this.results = results;
		}

		public Map<Object, Parameter> getBindings() {
			return bindings;
		}

		public String getSql() {
			return sql;
		}

		public int getFetchSize() {
			return fetchSize;
		}

		@Override
		public Statement add() {
			return this;
		}

		@Override
		public Statement bind(int index, Object o) {
			this.bindings.put(index, Parameter.from(o));
			return this;
		}

		@Override
		public Statement bind(String identifier, Object o) {
			this.bindings.put(identifier, Parameter.from(o));
			return this;
		}

		@Override
		public Statement bindNull(int index, Class<?> type) {
			this.bindings.put(index, Parameter.empty(type));
			return this;
		}

		@Override
		public Statement bindNull(String identifier, Class<?> type) {
			this.bindings.put(identifier, Parameter.empty(type));
			return this;
		}

		@Override
		public Statement fetchSize(int rows) {
			fetchSize = rows;
			return this;
		}

		@Override
		public Flux<Result> execute() {
			return Flux.fromIterable(results).doOnSubscribe(subscription -> executedStatements.add(this));
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [sql='").append(sql).append('\'');
			sb.append(", bindings=").append(bindings);
			sb.append(']');
			return sb.toString();
		}
	}

}
