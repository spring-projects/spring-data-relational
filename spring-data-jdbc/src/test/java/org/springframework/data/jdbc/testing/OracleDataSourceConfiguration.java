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

import static org.awaitility.pollinterval.FibonacciPollInterval.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import org.testcontainers.containers.OracleContainer;

/**
 * {@link DataSource} setup for Oracle Database XE. Starts a docker container with a Oracle database.
 *
 * @see <a href=
 *      "https://blogs.oracle.com/oraclemagazine/deliver-oracle-database-18c-express-edition-in-containers">Oracle
 *      Docker Image</a>
 * @see <a href="https://www.testcontainers.org/modules/databases/oraclexe/">Testcontainers Oracle</a>
 * @author Thomas Lang
 * @author Jens Schauder
 */
@Configuration
@Profile("oracle")
public class OracleDataSourceConfiguration extends DataSourceConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(OracleDataSourceConfiguration.class);

	private static OracleContainer ORACLE_CONTAINER;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		if (ORACLE_CONTAINER == null) {

			OracleContainer container = new OracleContainer("springci/spring-data-oracle-xe-prebuild:18.4.0").withReuse(true);
			container.start();

			ORACLE_CONTAINER = container;
		}

		String jdbcUrl = ORACLE_CONTAINER.getJdbcUrl().replace(":xe", "/XEPDB1");

		DataSource dataSource = new DriverManagerDataSource(jdbcUrl, ORACLE_CONTAINER.getUsername(),
				ORACLE_CONTAINER.getPassword());

		// Oracle container says its ready but it's like with a cat that denies service and still wants food although it had
		// its food. Therefore, we make sure that we can properly establish a connection instead of trusting the cat
		// ...err... Oracle.
		Awaitility.await().atMost(5L, TimeUnit.MINUTES).pollInterval(fibonacci(TimeUnit.SECONDS))
				.ignoreException(SQLException.class).until(() -> {

					try (Connection connection = dataSource.getConnection()) {
						return true;
					}
				});

		return dataSource;
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
