package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.RegisterExtension;

import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;

/**
 * Transactional integration tests for {@link DatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
public class PostgresTransactionalDatabaseClientIntegrationTests
		extends AbstractTransactionalDatabaseClientIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = PostgresTestSupport.database();

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
