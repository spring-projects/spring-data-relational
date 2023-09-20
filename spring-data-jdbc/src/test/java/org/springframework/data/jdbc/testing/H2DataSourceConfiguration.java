/*
 * Copyright 2020-2023 the original author or authors.
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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * {@link DataSource} setup for H2.
 *
 * @author Mark Paluch
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.H2)
class H2DataSourceConfiguration {

	private final TestClass testClass;

	public H2DataSourceConfiguration(TestClass testClass) {
		this.testClass = testClass;
	}

	@Bean
	DataSource dataSource() {

		return new EmbeddedDatabaseBuilder() //
				.generateUniqueName(true) //
				.setType(EmbeddedDatabaseType.H2) //
				.setScriptEncoding("UTF-8") //
				.ignoreFailedDrops(true) //
				.addScript(TestUtils.createScriptName(testClass.getTestClass(), "h2")) //
				.build();
	}
}
