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

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.NoTransactionException;

/**
 * Integration tests for {@link TransactionalDatabaseClient}.
 *
 * @author Mark Paluch
 */
public class TransactionalDatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

		connectionFactory = createConnectionFactory();

		String tableToCreate = "CREATE TABLE IF NOT EXISTS legoset (\n"
				+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" + "    name        varchar(255) NOT NULL,\n"
				+ "    manual      integer NULL\n" + ");";

		jdbc = createJdbcTemplate(createDataSource());
		jdbc.execute(tableToCreate);
		jdbc.execute("DELETE FROM legoset");
	}

	@Test
	public void executeInsertInManagedTransaction() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Integer> integerFlux = databaseClient.inTransaction(db -> {

			return db.execute().sql("INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)") //
					.bind(0, 42055) //
					.bind(1, "SCHAUFELRADBAGGER") //
					.bindNull("$3", Integer.class) //
					.fetch().rowsUpdated();
		});

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void executeInsertInAutoCommitTransaction() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Mono<Integer> integerFlux = databaseClient.execute()
				.sql("INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)") //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull("$3", Integer.class) //
				.fetch().rowsUpdated();

		integerFlux.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM legoset")).containsEntry("id", 42055);
	}

	@Test
	public void shouldManageUserTransaction() {

		Queue<Long> transactionIds = new ArrayBlockingQueue<>(5);
		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Long> txId = databaseClient.execute().sql("SELECT txid_current();").exchange()
				.flatMapMany(it -> it.extract((r, md) -> r.get(0, Long.class)).all());

		Mono<Void> then = databaseClient.enableTransactionSynchronization(databaseClient.beginTransaction() //
				.thenMany(txId.concatWith(txId).doOnNext(transactionIds::add)) //
				.then(databaseClient.rollbackTransaction()));

		then.as(StepVerifier::create) //
				.verifyComplete();

		List<Long> listOfTxIds = new ArrayList<>(transactionIds);
		assertThat(listOfTxIds).hasSize(2);
		assertThat(listOfTxIds).containsExactly(listOfTxIds.get(1), listOfTxIds.get(0));
	}

	@Test
	public void userTransactionManagementShouldFailWithoutSynchronizer() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Mono<Void> then = databaseClient.beginTransaction().then(databaseClient.rollbackTransaction());

		then.as(StepVerifier::create) //
				.consumeErrorWith(exception -> {

					assertThat(exception).isInstanceOf(NoTransactionException.class)
							.hasMessageContaining("Transaction management is not enabled");
				}).verify();
	}

	@Test
	public void shouldRollbackTransaction() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Integer> integerFlux = databaseClient.inTransaction(db -> {

			return db.execute().sql("INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)") //
					.bind(0, 42055) //
					.bind(1, "SCHAUFELRADBAGGER") //
					.bindNull("$3", Integer.class) //
					.fetch().rowsUpdated().then(Mono.error(new IllegalStateException("failed")));
		});

		integerFlux.as(StepVerifier::create) //
				.expectError(IllegalStateException.class) //
				.verify();

		assertThat(jdbc.queryForMap("SELECT count(*) FROM legoset")).containsEntry("count", 0L);
	}

	@Test
	public void emitTransactionIds() {

		TransactionalDatabaseClient databaseClient = TransactionalDatabaseClient.create(connectionFactory);

		Flux<Long> transactionIds = databaseClient.inTransaction(db -> {

			Flux<Long> txId = db.execute().sql("SELECT txid_current();").exchange()
					.flatMapMany(it -> it.extract((r, md) -> r.get(0, Long.class)).all());
			return txId.concatWith(txId);
		});

		transactionIds.collectList().as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(2);
					assertThat(actual).containsExactly(actual.get(1), actual.get(0));
				}) //
				.verifyComplete();
	}
}
