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

import javax.sql.DataSource;

import org.springframework.beans.factory.FactoryBean;

/**
 * @author Jens Schauder
 */
abstract class DataSourceFactoryBean implements FactoryBean<DataSource> {

	private final Class<?> testClass;

	DataSourceFactoryBean(Class<?> testClass) {
		this.testClass = testClass;
	}

	private DataSource createDataSource() {
		return create(createScriptName(testClass, scriptSuffix()));
	}

	abstract String scriptSuffix();

	abstract DataSource create(String scriptName);

	private String createScriptName(Class<?> testClass, String databaseType) {

		return String.format( //
				"%s/%s-%s.sql", //
				testClass.getPackage().getName(), //
				testClass.getSimpleName(), //
				databaseType.toLowerCase());
	}

	@Override
	public DataSource getObject() throws Exception {
		return createDataSource();
	}

	@Override
	public Class<?> getObjectType() {
		return DataSource.class;
	}
}
