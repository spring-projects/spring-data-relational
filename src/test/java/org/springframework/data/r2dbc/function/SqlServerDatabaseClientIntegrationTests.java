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

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.SqlServerTestSupport;

/**
 * Integration tests for {@link DatabaseClient} against Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
public class SqlServerDatabaseClientIntegrationTests extends AbstractDatabaseClientIntegrationTests {

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
}
