package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.springframework.data.r2dbc.core.TransactionalDatabaseClient;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;

/**
 * Integration tests for {@link TransactionalDatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
public class PostgresTransactionalDatabaseClientIntegrationTests
		extends AbstractTransactionalDatabaseClientIntegrationTests {

	@ClassRule public static final ExternalDatabase database = PostgresTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return PostgresTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return PostgresTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return PostgresTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	protected String getCurrentTransactionIdStatement() {
		return "SELECT txid_current();";
	}
}
