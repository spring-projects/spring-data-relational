/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.jdbc.testing;

import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.MSSQLServerContainer;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * {@link DataSource} setup for PostgreSQL.
 * <p>
 * Configuration for a MSSQL Datasource.
 *
 * @author Thomas Lang
 * @author Mark Paluch
 * @author Jens Schauder
 * @see <a href="https://github.com/testcontainers/testcontainers-java/tree/master/modules/mssqlserver"></a>
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.SQL_SERVER)
public class MsSqlDataSourceConfiguration extends DataSourceConfiguration {

	public static final String MS_SQL_SERVER_VERSION = "mcr.microsoft.com/mssql/server:2022-CU5-ubuntu-20.04";
	private static MSSQLServerContainer<?> MSSQL_CONTAINER;

	public MsSqlDataSourceConfiguration(TestClass testClass, Environment environment) {
		super(testClass, environment);
	}

	@Override
	protected DataSource createDataSource() {

		if (MSSQL_CONTAINER == null) {

			MSSQLServerContainer<?> container = new MSSQLServerContainer<>(MS_SQL_SERVER_VERSION) //
					.withReuse(true);
			container.start();

			MSSQL_CONTAINER = container;
		}

		SQLServerDataSource sqlServerDataSource = new SQLServerDataSource();
		sqlServerDataSource.setURL(MSSQL_CONTAINER.getJdbcUrl());
		sqlServerDataSource.setUser(MSSQL_CONTAINER.getUsername());
		sqlServerDataSource.setPassword(MSSQL_CONTAINER.getPassword());

		return sqlServerDataSource;
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
