/*
 * Copyright 2017-2023 the original author or authors.
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

import static org.awaitility.pollinterval.FibonacciPollInterval.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.awaitility.Awaitility;
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
@Configuration(proxyBeanMethods = false)
abstract class DataSourceConfiguration {

	private static final Log LOG = LogFactory.getLog(DataSourceConfiguration.class);

	private final TestClass testClass;
	private final Environment environment;

	public DataSourceConfiguration(TestClass testClass, Environment environment) {
		this.testClass = testClass;
		this.environment = environment;
	}

	@Bean
	DataSource dataSource() {
		DataSource dataSource = createDataSource();
		verifyConnection(dataSource);
		return dataSource;
	}

	@Bean
	DataSourceInitializer initializer(DataSource dataSource) {

		DataSourceInitializer initializer = new DataSourceInitializer();
		initializer.setDataSource(dataSource);

		String[] activeProfiles = environment.getActiveProfiles();
		String profile = getDatabaseProfile(activeProfiles);

		ClassPathResource script = new ClassPathResource(TestUtils.createScriptName(testClass.getTestClass(), profile));
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator(script);
		customizePopulator(populator);
		initializer.setDatabasePopulator(populator);

		return initializer;
	}

	private static String getDatabaseProfile(String[] activeProfiles) {

		List<String> validDbs = Arrays.stream(DatabaseType.values()).map(DatabaseType::getProfile).toList();
		for (String profile : activeProfiles) {
			if (validDbs.contains(profile)) {
				return profile;
			}
		}

		return "";
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

	private void verifyConnection(DataSource dataSource) {

		Awaitility.await() //
				.atMost(5L, TimeUnit.MINUTES) //
				.pollInterval(fibonacci(TimeUnit.SECONDS)) //
				.ignoreExceptions() //
				.until(() -> {

					if (LOG.isDebugEnabled()) {
						LOG.debug(String.format("Verifying connectivity to %s...", dataSource));
					}

					try (Connection connection = dataSource.getConnection()) {
						return true;
					}
				});

		LOG.debug("Connectivity verified");
	}
}
