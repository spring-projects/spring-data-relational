/*
 * Copyright 2023 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

/**
 * {@link ContextCustomizer} to select a specific configuration profile based on the {@code @EnabledOnDatabase}
 * annotation.
 *
 * @author Mark Paluch
 * @see EnabledOnDatabase
 */
public class EnabledOnDatabaseCustomizer implements ContextCustomizer {

	private final Class<?> testClass;

	public EnabledOnDatabaseCustomizer(Class<?> testClass) {
		this.testClass = testClass;
	}

	@Override
	public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {

		MergedAnnotation<EnabledOnDatabase> annotation = MergedAnnotations.from(testClass, SearchStrategy.TYPE_HIERARCHY)
				.get(EnabledOnDatabase.class);

		if (annotation.isPresent()) {

			DatabaseType value = annotation.getEnum("value", DatabaseType.class);

			customizeEnvironment(context.getEnvironment(), value);
		}
	}

	static void customizeEnvironment(ConfigurableEnvironment environment, DatabaseType value) {

		List<String> profiles = Arrays.asList(environment.getActiveProfiles());

		for (DatabaseType databaseType : DatabaseType.values()) {

			if (profiles.contains(databaseType.getProfile())) {
				return;
			}
		}

		environment.addActiveProfile(value.getProfile());
	}

	public static class EnabledOnDatabaseCustomizerFactory implements ContextCustomizerFactory {

		@Override
		public ContextCustomizer createContextCustomizer(Class<?> testClass,
				List<ContextConfigurationAttributes> configAttributes) {
			return new EnabledOnDatabaseCustomizer(testClass);
		}

	}

}
