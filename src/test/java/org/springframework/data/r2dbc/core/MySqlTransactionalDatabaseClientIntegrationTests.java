/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;

import javax.sql.DataSource;

import org.junit.ClassRule;

import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.MySqlTestSupport;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Transactional integration tests for {@link DatabaseClient} against MySQL.
 *
 * @author Mark Paluch
 */
public class MySqlTransactionalDatabaseClientIntegrationTests
		extends AbstractTransactionalDatabaseClientIntegrationTests {

	@ClassRule public static final ExternalDatabase database = MySqlTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return MySqlTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return MySqlTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return MySqlTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	protected Mono<Void> prepareForTransaction(DatabaseClient client) {

		/*
		 * We have to execute a sql statement first.
		 * Otherwise MySql don't have a transaction id.
		 * And we need to delay emitting the result so that MySql has time to write the transaction id, which is done in
		 * batches every now and then.
		 * @see: https://dev.mysql.com/doc/refman/5.7/en/innodb-information-schema-internal-data.html
		 */
		return client.sql(getInsertIntoLegosetStatement()) //
				.bind(0, 42055) //
				.bind(1, "SCHAUFELRADBAGGER") //
				.bindNull(2, Integer.class) //
				.fetch().rowsUpdated() //
				.delayElement(Duration.ofMillis(50)) //
				.then();
	}

	@Override
	protected String getCurrentTransactionIdStatement() {
		return "SELECT tx.trx_id FROM information_schema.innodb_trx tx WHERE tx.trx_mysql_thread_id = connection_id()";
	}
}
