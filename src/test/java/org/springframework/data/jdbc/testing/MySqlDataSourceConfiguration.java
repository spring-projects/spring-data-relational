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

import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.script.ScriptException;
import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.jdbc.ext.ScriptUtils;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * {@link DataSource} setup for MySQL.
 *
 * Starts a docker container with a MySql database and sets up a database name "test" in it.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Sedat Gokcen
 */
@Configuration
@Profile("mysql")
class MySqlDataSourceConfiguration extends DataSourceConfiguration {

	private static final MySQLContainer MYSQL_CONTAINER = new MySQLContainer().withConfigurationOverride("");

	static {
		MYSQL_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {

		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl(MYSQL_CONTAINER.getJdbcUrl());
		dataSource.setUser(MYSQL_CONTAINER.getUsername());
		dataSource.setPassword(MYSQL_CONTAINER.getPassword());
		dataSource.setDatabaseName(MYSQL_CONTAINER.getDatabaseName());

		return dataSource;
	}

	@PostConstruct
	public void initDatabase() throws SQLException, ScriptException {
		ScriptUtils.executeSqlScript(createDataSource().getConnection(), null, "DROP DATABASE test;CREATE DATABASE test;");
	}
}
