/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * {@link DataSource} setup for PostgreSQL.
 *
 * Starts a docker container with a Postgres database.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Sedat Gokcen
 */
@Configuration
@Profile("postgres")
public class PostgresDataSourceConfiguration extends DataSourceConfiguration {

	private static final PostgreSQLContainer POSTGRESQL_CONTAINER = new PostgreSQLContainer();

	static {
		POSTGRESQL_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setUrl(POSTGRESQL_CONTAINER.getJdbcUrl());
		dataSource.setUser(POSTGRESQL_CONTAINER.getUsername());
		dataSource.setPassword(POSTGRESQL_CONTAINER.getPassword());

		return dataSource;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceFactoryBean#customizePopulator(org.springframework.jdbc.datasource.init.ResourceDatabasePopulator)
	 */
	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
