/*
 * Copyright 2017-2024 the original author or authors.
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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.MariaDBContainer;

/**
 * {@link DataSource} setup for MariaDB. Starts a Docker-container with a MariaDB database, and sets up database "test".
 *
 * @author Christoph Preißner
 * @author Mark Paluch
 * @author Jens Schauder
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.MARIADB)
class MariaDBDataSourceConfiguration extends DataSourceConfiguration implements InitializingBean {

	private static MariaDBContainer<?> MARIADB_CONTAINER;

	public MariaDBDataSourceConfiguration(TestClass testClass, Environment environment) {
		super(testClass, environment);
	}

	@Override
	protected DataSource createDataSource() {

		if (MARIADB_CONTAINER == null) {

			MariaDBContainer<?> container = new MariaDBContainer<>("mariadb:10.8.3").withUsername("root").withPassword("")
					.withConfigurationOverride("");
			container.start();

			MARIADB_CONTAINER = container;
		}

		try {

			MariaDbDataSource dataSource = new MariaDbDataSource();
			dataSource.setUrl(MARIADB_CONTAINER.getJdbcUrl());
			dataSource.setUser(MARIADB_CONTAINER.getUsername());
			dataSource.setPassword(MARIADB_CONTAINER.getPassword());
			return dataSource;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		try (Connection connection = createDataSource().getConnection()) {
			ScriptUtils.executeSqlScript(connection,
					new ByteArrayResource("DROP DATABASE test;CREATE DATABASE test;".getBytes()));
		}
	}
}
