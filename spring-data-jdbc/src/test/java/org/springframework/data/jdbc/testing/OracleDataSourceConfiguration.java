/*
 * Copyright 2018 the original author or authors.
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

import oracle.jdbc.pool.OracleDataSource;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.OracleContainer;

/**
 * {@link DataSource} setup for Oracle.
 *
 * Starts a docker container with an Oracle database.
 *
 * @author Michael Bahr
 */
@Configuration
@Profile("oracle")
public class OracleDataSourceConfiguration extends DataSourceConfiguration {

	private static final OracleContainer ORACLE_CONTAINER = new OracleContainer();

	static {
		ORACLE_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {
		try {
			OracleDataSource dataSource = new OracleDataSource();
			dataSource.setURL(ORACLE_CONTAINER.getJdbcUrl());
			dataSource.setUser(ORACLE_CONTAINER.getUsername());
			dataSource.setPassword(ORACLE_CONTAINER.getPassword());
			return dataSource;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
		populator.setSeparator("/");
	}

}
