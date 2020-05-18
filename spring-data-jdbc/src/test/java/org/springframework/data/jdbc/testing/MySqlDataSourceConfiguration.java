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

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import org.testcontainers.containers.MySQLContainer;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * {@link DataSource} setup for MySQL. Starts a docker container with a MySql database and sets up a database name
 * "test" in it.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Sedat Gokcen
 * @author Mark Paluch
 */
@Configuration
@Profile("mysql")
class MySqlDataSourceConfiguration extends DataSourceConfiguration {

	private static MySQLContainer<?> MYSQL_CONTAINER;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		if (MYSQL_CONTAINER == null) {

			MySQLContainer<?> container = new MySQLContainer<>()
					.withUsername("root")
					.withPassword("")
					.withConfigurationOverride("");

			container.start();

			MYSQL_CONTAINER = container;
		}

		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl(MYSQL_CONTAINER.getJdbcUrl());
		dataSource.setUser(MYSQL_CONTAINER.getUsername());
		dataSource.setPassword(MYSQL_CONTAINER.getPassword());
		dataSource.setDatabaseName(MYSQL_CONTAINER.getDatabaseName());

		return dataSource;
	}

	@PostConstruct
	public void initDatabase() throws SQLException {

		try (Connection connection = createDataSource().getConnection()) {
			ScriptUtils.executeSqlScript(connection,
					new ByteArrayResource("DROP DATABASE test;CREATE DATABASE test;".getBytes()));
		}
	}
}
