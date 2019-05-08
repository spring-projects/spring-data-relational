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

import static org.assertj.core.api.Assertions.*;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import io.r2dbc.spi.ConnectionFactory;
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
import org.springframework.data.r2dbc.function.connectionfactory.ConnectionFactoryTransactionManager;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Abstract base class for integration tests for {@link TransactionalDatabaseClient}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class AbstractTransactionalDatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	AnnotationConfigApplicationContext context;
	TransactionalService service;

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
		} catch (DataAccessException e) {
		}
		jdbc.execute(getCreateTableStatement());
		jdbc.execute("DELETE FROM legoset");
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

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Integer> integerFlux = databaseClient.inTransaction(db -> db //
				.execute() //
				.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
		);

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void executeInsertInAutoCommitTransaction() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Mono<Integer> integerFlux = databaseClient.execute().sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated();

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test // gh-2
	public void shouldManageUserTransaction() {

		Queue<Long> transactionIds = new ArrayBlockingQueue<>(5);
		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Long> txId = databaseClient //
				.execute() //
				.sql(getCurrentTransactionIdStatement()) //
				.map((r, md) -> r.get(0, Long.class)) //
				.all();

		Mono<Void> then = databaseClient.enableTransactionSynchronization(databaseClient.beginTransaction() //
				.thenMany(txId.concatWith(txId).doOnNext(transactionIds::add)) //
				.then(databaseClient.rollbackTransaction()));

		then.as(StepVerifier::create) //
				.verifyComplete();

		List<Long> listOfTxIds = new ArrayList<>(transactionIds);
		assertThat(listOfTxIds).hasSize(2);
		assertThat(listOfTxIds).containsExactly(listOfTxIds.get(1), listOfTxIds.get(0));
	}

	@Test // gh-2
	public void userTransactionManagementShouldFailWithoutSynchronizer() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Mono<Void> then = databaseClient.beginTransaction().then(databaseClient.rollbackTransaction());

		then.as(StepVerifier::create) //
				.consumeErrorWith(exception -> {

					assertThat(exception).isInstanceOf(NoTransactionException.class)
							.hasMessageContaining("Transaction management is not enabled");
				}).verify();
	}

	@Test // gh-2
	public void shouldRollbackTransaction() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Integer> integerFlux = databaseClient.inTransaction(db -> {

			return db.execute().sql(getInsertIntoLegosetStatement()) //
					.bind(0, 42055) //
					.bind(1, "SCHAUFELRADBAGGER") //
					.bindNull(2, Integer.class) //
					.fetch().rowsUpdated() //
					.then(Mono.error(new IllegalStateException("failed")));
		});

		integerFlux.as(StepVerifier::create) //
				.expectError(IllegalStateException.class) //
				.verify();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM legoset", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test // gh-2, gh-75, gh-107
	public void emitTransactionIds() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		TransactionalOperator transactionalOperator = TransactionalOperator
				.create(new ConnectionFactoryTransactionManager(connectionFactory), new DefaultTransactionDefinition());

		Flux<Object> txId = databaseClient.execute() //
				.sql(getCurrentTransactionIdStatement()) //
				.map((row, md) -> row.get(0)) //
				.all();

		Flux<Object> transactionIds = prepareForTransaction(databaseClient).thenMany(txId.concatWith(txId)) //
				.as(transactionalOperator::transactional);

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
				.create(new ConnectionFactoryTransactionManager(connectionFactory), new DefaultTransactionDefinition());

		Flux<Integer> integerFlux = databaseClient.execute() //
				.sql(getInsertIntoLegosetStatement()) //
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

	@Test //gh-107
	public void emitTransactionIdsUsingManagedTransactions() {

		service.emitTransactionIds(prepareForTransaction(service.getDatabaseClient()), getCurrentTransactionIdStatement()).collectList().as(StepVerifier::create) //
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
			return new ConnectionFactoryTransactionManager(connectionFactory);
		}
	}

	static class TransactionalService {

		private DatabaseClient databaseClient;

		public TransactionalService(DatabaseClient databaseClient) {
			this.databaseClient = databaseClient;
		}

		@Transactional
		public Flux<Object> emitTransactionIds(Mono<Void> prepareTransaction, String idStatement) {

			Flux<Object> txId = databaseClient.execute() //
					.sql(idStatement) //
					.map((row, md) -> row.get(0)) //
					.all();

			return prepareTransaction.thenMany(txId.concatWith(txId));
		}


		@Transactional
		public Flux<Integer> shouldRollbackTransactionUsingTransactionalOperator(String insertStatement) {

			return databaseClient.execute().sql(insertStatement) //
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
