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

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;

/**
 * @author Jens Schauder
 */
@Component
@Profile("postgres")
public class PostgresDataSourceFactoryBean extends DataSourceFactoryBean {

	public static final String URL = "jdbc:postgresql:///postgres";
	private static final DefaultResourceLoader resourceLoader = new DefaultResourceLoader();

	PostgresDataSourceFactoryBean(Class<?> testClass) {
		super(testClass);
	}

	@Override
	protected String scriptSuffix() {
		return "postgres";
	}

	@Override
	DataSource create(String scriptName) {
		return setupDatabase(scriptName);
	}

	private DataSource setupDatabase(String scriptName) {

		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setUrl(URL);

		try (Connection connection = ds.getConnection()) {

			ScriptUtils.executeSqlScript(connection, new EncodedResource(resourceLoader.getResource(scriptName)), false, true,
					"--", ";", "/*", "*/");
		} catch (SQLException e) {
			throw new RuntimeException("Couldn't setup database", e);
		}

		return ds;
	}
}
