/*
 * Copyright 2017-2020 the original author or authors.
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

import oracle.jdbc.pool.OracleDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.OracleContainer;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * {@link DataSource} setup for Oracle Database XE. Starts a docker container with a Oracle database.
 *
 * @see <a href=
 *      "https://blogs.oracle.com/oraclemagazine/deliver-oracle-database-18c-express-edition-in-containers">Oracle
 *      Docker Image</a>
 * @see <a href="https://www.testcontainers.org/modules/databases/oraclexe/">Testcontainers Oracle</a>
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Sedat Gokcen
 * @author Mark Paluch
 */
@Configuration
@Profile("oracle")
public class OracleDataSourceConfiguration extends DataSourceConfiguration {

	private static OracleContainer ORACLE_CONTAINER;

	@BeforeAll
	public static void startup() {
		ORACLE_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		if (ORACLE_CONTAINER == null) {

			OracleContainer container = new OracleContainer("name_of_your_oracle_xe_image");
			container.start();

			ORACLE_CONTAINER = container;
		}

		try {
			OracleDataSource oracleDataSource = new OracleDataSource();
			oracleDataSource.setURL(ORACLE_CONTAINER.getJdbcUrl());
			oracleDataSource.setUser(ORACLE_CONTAINER.getUsername());
			oracleDataSource.setPassword(ORACLE_CONTAINER.getPassword());
			return oracleDataSource;
		} catch (SQLException throwables) {
			throwables.printStackTrace();
			return null;
		}

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
