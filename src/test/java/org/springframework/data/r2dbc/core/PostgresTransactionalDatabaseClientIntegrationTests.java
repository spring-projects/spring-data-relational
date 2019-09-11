package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.junit.Ignore;

import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;

/**
 * Transactional integration tests for {@link DatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
@Ignore("https://github.com/r2dbc/r2dbc-postgresql/issues/151")
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
