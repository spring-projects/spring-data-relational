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

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * {@link DataSource} setup for PostgreSQL. Starts a docker container with a Postgres database.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Sedat Gokcen
 * @author Mark Paluch
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.POSTGRES)
public class PostgresDataSourceConfiguration extends DataSourceConfiguration {

	private static PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

	public PostgresDataSourceConfiguration(TestClass testClass, Environment environment) {
		super(testClass, environment);
	}

	@Override
	protected DataSource createDataSource() {

		if (POSTGRESQL_CONTAINER == null) {

			PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:14.3");
			container.start();

			POSTGRESQL_CONTAINER = container;
		}

		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUrl(POSTGRESQL_CONTAINER.getJdbcUrl());
		dataSource.setUser(POSTGRESQL_CONTAINER.getUsername());
		dataSource.setPassword(POSTGRESQL_CONTAINER.getPassword());

		return dataSource;
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
