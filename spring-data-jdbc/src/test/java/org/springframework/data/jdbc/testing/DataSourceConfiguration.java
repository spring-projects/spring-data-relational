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

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * Basic configuration expecting subclasses to provide a {@link DataSource} via {@link #createDataSource()} to be
 * exposed to the {@link ApplicationContext}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@Configuration
abstract class DataSourceConfiguration {

	@Autowired Class<?> testClass;
	@Autowired Environment environment;

	@Bean
	DataSource dataSource() {
		return createDataSource();
	}

	@Bean
	DataSourceInitializer initializer() {

		DataSourceInitializer initializer = new DataSourceInitializer();
		initializer.setDataSource(dataSource());

		String[] activeProfiles = environment.getActiveProfiles();
		String profile = activeProfiles.length == 0 ? "" : activeProfiles[0];

		ClassPathResource script = new ClassPathResource(TestUtils.createScriptName(testClass, profile));
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator(script);
		customizePopulator(populator);
		initializer.setDatabasePopulator(populator);

		return initializer;
	}

	/**
	 * Return the {@link DataSource} to be exposed as a Spring bean.
	 *
	 * @return
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Callback to customize the {@link ResourceDatabasePopulator} before it will be applied to the {@link DataSource}. It
	 * will be pre-populated with a SQL script derived from the name of the current test class and the activated Spring
	 * profile.
	 *
	 * @param populator will never be {@literal null}.
	 */
	protected void customizePopulator(ResourceDatabasePopulator populator) {}
}
