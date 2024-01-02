/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.support;

import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.SqlServerTestSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link SimpleR2dbcRepository} against Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@DisabledOnOs(architectures = "aarch64")
public class SqlServerSimpleR2dbcRepositoryIntegrationTests extends AbstractSimpleR2dbcRepositoryIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = SqlServerTestSupport.database();

	@Configuration
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Override
		public ConnectionFactory connectionFactory() {
			return SqlServerTestSupport.createConnectionFactory(database);
		}
	}

	@Override
	protected DataSource createDataSource() {
		return SqlServerTestSupport.createDataSource(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return SqlServerTestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}
}
