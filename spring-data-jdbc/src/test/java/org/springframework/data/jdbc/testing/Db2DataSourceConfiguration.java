/*
 * Copyright 2020-2023 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import org.testcontainers.containers.Db2Container;

/**
 * {@link DataSource} setup for DB2.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Configuration
@Profile("db2")
class Db2DataSourceConfiguration extends DataSourceConfiguration {

	private static final Log LOG = LogFactory.getLog(Db2DataSourceConfiguration.class);

	private static Db2Container DB_2_CONTAINER;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		if (DB_2_CONTAINER == null) {

			LOG.info("DB2 starting...");
			Db2Container container = new Db2Container().withReuse(true);
			container.start();
			LOG.info("DB2 started");

			DB_2_CONTAINER = container;
		}

		return new DriverManagerDataSource(DB_2_CONTAINER.getJdbcUrl(),
				DB_2_CONTAINER.getUsername(), DB_2_CONTAINER.getPassword());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.customizePopulator#createDataSource(org.springframework.jdbc.datasource.init.ResourceDatabasePopulator)
	 */
	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
