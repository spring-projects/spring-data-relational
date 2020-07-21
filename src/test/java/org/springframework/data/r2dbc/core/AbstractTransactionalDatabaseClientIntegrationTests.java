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

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * Abstract base class for transactional integration tests for {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class AbstractTransactionalDatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	AnnotationConfigApplicationContext context;
	TransactionalService service;

	DatabaseClient databaseClient;
	R2dbcTransactionManager transactionManager;
	TransactionalOperator rxtx;

	@Before
	public void before() {

		connectionFactory = createConnectionFactory();

		context = new AnnotationConfigApplicationContext();
		context.registerBean("theConnectionFactory", ConnectionFactory.class, () -> connectionFactory);
		context.register(Config.class, TransactionalService.class);
		context.refresh();

		service = context.getBean(TransactionalService.class);

		jdbc = createJdbcTemplate(createDataSource());
		try {
			jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}
		jdbc.execute(getCreateTableStatement());
		jdbc.execute("DELETE FROM legoset");

		databaseClient = DatabaseClient.create(connectionFactory);
		transactionManager = new R2dbcTransactionManager(connectionFactory);
		rxtx = TransactionalOperator.create(transactionManager);
	}

	@After
	public void tearDown() {
		context.close();
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Returns the the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	/**
	 * Get a parameterized {@code INSERT INTO legoset} statement setting id, name, and manual values.
	 */
	protected String getInsertIntoLegosetStatement() {
		return "INSERT INTO legoset (id, name, manual) VALUES(:id, :name, :manual)";
	}

	/**
	 * Some Databases require special treatment to convince them to start a transaction. Some even start a transaction but
	 * store its id async so that it might show up a little late.
	 *
	 * @param client the client to use
	 * @return an empty {@link Mono} by default.
	 */
	protected Mono<Void> prepareForTransaction(DatabaseClient client) {
		return Mono.empty();
	}

	/**
	 * Get a statement that returns the current transactionId.
	 */
	protected abstract String getCurrentTransactionIdStatement();

	@Test // gh-2
	public void executeInsertInManagedTransaction() {

		Flux<Integer> integerFlux = databaseClient //
				.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated().flux().as(rxtx::transactional);

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).hasEntrySatisfying("id", numberOf(42055));
	}

	@Test // gh-2
	public void executeInsertInAutoCommitTransaction() {

		Flux<Integer> integerFlux = databaseClient.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated().flux().as(rxtx::transactional);

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).hasEntrySatisfying("id", numberOf(42055));
	}

	@Test // gh-2
	public void shouldRollbackTransaction() {

		Mono<Object> integerFlux = databaseClient.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.then(Mono.error(new IllegalStateException("failed"))).as(rxtx::transactional);

		integerFlux.as(StepVerifier::create) //
				.expectError(IllegalStateException.class) //
				.verify();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test // gh-2, gh-75, gh-107
	public void emitTransactionIds() {

		Flux<Object> txId = databaseClient.sql(getCurrentTransactionIdStatement()) //
				.map((row, md) -> row.get(0)) //
				.all();

		Flux<Object> transactionIds = prepareForTransaction(databaseClient).thenMany(txId.concatWith(txId)) //
				.as(rxtx::transactional);

		transactionIds.collectList().as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(2);
					assertThat(actual.get(0)).isEqualTo(actual.get(1));
				}) //
				.verifyComplete();
	}

	@Test // gh-107
	public void shouldRollbackTransactionUsingTransactionalOperator() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		TransactionalOperator transactionalOperator = TransactionalOperator
				.create(new R2dbcTransactionManager(connectionFactory), new DefaultTransactionDefinition());

		Flux<Integer> integerFlux = databaseClient.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.thenMany(Mono.fromSupplier(() -> {
					throw new IllegalStateException("failed");
				}));

		integerFlux.as(transactionalOperator::transactional) //
				.as(StepVerifier::create) //
				.expectError(IllegalStateException.class) //
				.verify();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test // gh-107
	public void emitTransactionIdsUsingManagedTransactions() {

		service.emitTransactionIds(prepareForTransaction(service.getDatabaseClient()), getCurrentTransactionIdStatement())
				.collectList().as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(2);
					assertThat(actual.get(0)).isEqualTo(actual.get(1));
				}) //
				.verifyComplete();
	}

	@Test // gh-107
	public void shouldRollbackTransactionUsingManagedTransactions() {

		service.shouldRollbackTransactionUsingTransactionalOperator(getInsertIntoLegosetStatement())
				.as(StepVerifier::create) //
				.expectError(IllegalStateException.class) //
				.verify();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	private Condition<? super Object> numberOf(int expected) {
		return new Condition<>(it -> {
			return it instanceof Number && ((Number) it).intValue() == expected;
		}, "Number  %d", expected);
	}

	@Configuration
	@EnableTransactionManagement
	static class Config extends AbstractR2dbcConfiguration {

		@Autowired GenericApplicationContext context;

		@Override
		public ConnectionFactory connectionFactory() {
			return lookup();
		}

		ConnectionFactory lookup() {
			return context.getBean("theConnectionFactory", ConnectionFactory.class);
		}

		@Bean
		ReactiveTransactionManager txMgr(ConnectionFactory connectionFactory) {
			return new R2dbcTransactionManager(connectionFactory);
		}
	}

	static class TransactionalService {

		private DatabaseClient databaseClient;

		public TransactionalService(DatabaseClient databaseClient) {
			this.databaseClient = databaseClient;
		}

		@Transactional
		public Flux<Object> emitTransactionIds(Mono<Void> prepareTransaction, String idStatement) {

			Flux<Object> txId = databaseClient.sql(idStatement) //
					.map((row, md) -> row.get(0)) //
					.all();

			return prepareTransaction.thenMany(txId.concatWith(txId));
		}

		@Transactional
		public Flux<Integer> shouldRollbackTransactionUsingTransactionalOperator(String insertStatement) {

			return databaseClient.sql(insertStatement) //
					.bind(0, 42055) //
					.bind(1, "SCHAUFELRADBAGGER") //
					.bindNull(2, Integer.class) //
					.fetch().rowsUpdated() //
					.thenMany(Mono.fromSupplier(() -> {
						throw new IllegalStateException("failed");
					}));
		}

		public DatabaseClient getDatabaseClient() {
			return databaseClient;
		}
	}
}
