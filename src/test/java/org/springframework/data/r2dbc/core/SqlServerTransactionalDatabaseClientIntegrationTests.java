package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.springframework.data.r2dbc.core.TransactionalDatabaseClient;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.SqlServerTestSupport;

/**
 * Integration tests for {@link TransactionalDatabaseClient} against Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
public class SqlServerTransactionalDatabaseClientIntegrationTests
		extends AbstractTransactionalDatabaseClientIntegrationTests {

	@ClassRule public static final ExternalDatabase database = SqlServerTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return SqlServerTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return SqlServerTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return SqlServerTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	protected String getInsertIntoLegosetStatement() {
		return SqlServerTestSupport.INSERT_INTO_LEGOSET;
	}

	@Override
	protected String getCurrentTransactionIdStatement() {
		return "SELECT CURRENT_TRANSACTION_ID();";
	}
}
