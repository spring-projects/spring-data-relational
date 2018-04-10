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

import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.jdbc.ext.ScriptUtils;

/**
 * {@link DataSource} setup for MariaDB.
 *
 * Starts a Docker-container with a MariaDB database, and sets up database "test".

 * @author Christoph Preißner
 */
@Configuration
@Profile("mariadb")
class MariaDBDataSourceConfiguration extends DataSourceConfiguration {

	private static final MariaDBContainer MARIADB_CONTAINER = new MariaDBContainer().withConfigurationOverride("");

	static {
		MARIADB_CONTAINER.start();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
	 */
	@Override
	protected DataSource createDataSource() {
		try {
			MariaDbDataSource dataSource = new MariaDbDataSource();
			dataSource.setUrl(MARIADB_CONTAINER.getJdbcUrl());
			dataSource.setUser(MARIADB_CONTAINER.getUsername());
			dataSource.setPassword(MARIADB_CONTAINER.getPassword());
			return dataSource;
		} catch(SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}
	}

	@PostConstruct
	public void initDatabase() throws SQLException, ScriptException {
		ScriptUtils.executeSqlScript(createDataSource().getConnection(), null, "DROP DATABASE test;CREATE DATABASE test;");
	}
}
