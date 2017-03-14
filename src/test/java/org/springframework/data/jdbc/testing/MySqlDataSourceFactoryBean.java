/*
 * Copyright 2017 the original author or authors.
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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.context.annotation.Profile;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * @author Jens Schauder
 */
@Component
@Profile("mysql")
class MySqlDataSourceFactoryBean extends DataSourceFactoryBean {

	public static final String ROOT_URL = "jdbc:mysql:///?user=root";
	public static final String COULDN_T_CREATE_DATABASE = //
			"Couldn't create database. Maybe you don't have a MySql database running, reachable at %s";
	private static final DefaultResourceLoader resourceLoader = new DefaultResourceLoader();
	private static final String TEST_URL = "jdbc:mysql:///test?user=root";

	MySqlDataSourceFactoryBean(Class<?> testClass) {
		super(testClass);
	}

	@Override
	String scriptSuffix() {
		return "mysql";
	}

	@Override
	DataSource create(String scriptName) {

		createDatabase();
		return setupDatabase(scriptName);
	}

	private MysqlDataSource setupDatabase(String scriptName) {

		MysqlDataSource ds = new MysqlDataSource();
		ds.setUrl(TEST_URL);

		try (Connection connection = ds.getConnection()) {
			ScriptUtils.executeSqlScript(connection, resourceLoader.getResource(scriptName));
		} catch (SQLException e) {
			throw new RuntimeException("Couldn't setup database", e);
		}

		return ds;
	}

	private void createDatabase() {

		MysqlDataSource initialDs = new MysqlDataSource();
		initialDs.setUrl(ROOT_URL);

		try (Connection connection = initialDs.getConnection()) {
			ScriptUtils.executeSqlScript(connection, resourceLoader.getResource("create-mysql.sql"));
		} catch (SQLException e) {
			throw new RuntimeException(String.format(COULDN_T_CREATE_DATABASE, ROOT_URL), e);
		}
	}
}
