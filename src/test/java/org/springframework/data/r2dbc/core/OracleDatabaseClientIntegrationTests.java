/*
 * Copyright 2018-2021 the original author or authors.
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

import javax.sql.DataSource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.springframework.data.r2dbc.testing.EnabledOnClass;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.OracleTestSupport;

/**
 * Integration tests for {@link DatabaseClient} against Oracle.
 *
 * @author Mark Paluch
 */
@EnabledOnClass("oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl")
public class OracleDatabaseClientIntegrationTests extends AbstractDatabaseClientIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = OracleTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return OracleTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return OracleTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return OracleTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	@Disabled("https://github.com/oracle/oracle-r2dbc/issues/9")
	public void executeSelectNamedParameters() {}
}
