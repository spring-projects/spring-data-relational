/*
 * Copyright 2020-2025 the original author or authors.
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
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * {@link DataSource} setup for Oracle Database XE. Starts a docker container with an Oracle database.
 *
 * @see <a href= "https://blogs.oracle.com/oraclemagazine/deliver-oracle-database-18c-express-edition-in-containers">Oracle Docker Image</a>
 * @see <a href="https://www.testcontainers.org/modules/databases/oraclexe/">Testcontainers Oracle</a>
 * @author Thomas Lang
 * @author Jens Schauder
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.ORACLE)
public class OracleDataSourceConfiguration extends DataSourceConfiguration {

	private static final Log LOG = LogFactory.getLog(OracleDataSourceConfiguration.class);

	private static DataSource DATA_SOURCE;

	public OracleDataSourceConfiguration(TestClass testClass, Environment environment) {
		super(testClass, environment);
	}

	@Override
	protected synchronized DataSource createDataSource() {

		if (DATA_SOURCE == null) {

			LOG.info("Oracle starting...");
			DockerImageName dockerImageName = DockerImageName.parse("gvenzl/oracle-free:23.3-slim");
			OracleContainer container = new OracleContainer(dockerImageName) //
					.withStartupTimeoutSeconds(200) //
					.withReuse(true);
			container.start();
			LOG.info("Oracle started");

			initDb(container.getJdbcUrl(),container.getUsername(), container.getPassword());

			DATA_SOURCE = poolDataSource(new DriverManagerDataSource(container.getJdbcUrl(),
					container.getUsername(), container.getPassword()));
		}
		return DATA_SOURCE;
	}

	private DataSource poolDataSource(DataSource dataSource) {

		HikariConfig config = new HikariConfig();
		config.setDataSource(dataSource);

		config.setMaximumPoolSize(10);
		config.setIdleTimeout(30000);
		config.setMaxLifetime(600000);
		config.setConnectionTimeout(30000);

		return new HikariDataSource(config);
	}

	private void initDb(String jdbcUrl, String username, String password) {

		final DriverManagerDataSource dataSource = new DriverManagerDataSource(jdbcUrl, "SYSTEM",
				password);
		final JdbcTemplate jdbc = new JdbcTemplate(dataSource);
		jdbc.execute("GRANT ALL PRIVILEGES TO " + username);
	}

	@Override
	protected void customizePopulator(ResourceDatabasePopulator populator) {
		populator.setIgnoreFailedDrops(true);
	}
}
