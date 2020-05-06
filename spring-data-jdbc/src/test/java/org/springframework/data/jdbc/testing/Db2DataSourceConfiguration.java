/*
 * Copyright 2020 the original author or authors.
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
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.Db2Container;

/**
 * {@link DataSource} setup for DB2.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@Configuration
@Profile("db2")
class Db2DataSourceConfiguration extends DataSourceConfiguration {

	private static final Db2Container DB_2_CONTAINER = new Db2Container();

	static {
		DB_2_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource(DB_2_CONTAINER.getJdbcUrl(),
				DB_2_CONTAINER.getUsername(), DB_2_CONTAINER.getPassword());

		return dataSource;
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
