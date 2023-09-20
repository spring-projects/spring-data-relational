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

import static org.assertj.core.api.Assumptions.*;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import org.junit.platform.commons.util.AnnotationUtils;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.util.MultiValueMap;

/**
 * {@link Condition} and {@link TestExecutionListener} to test whether the required {@link DatabaseType} configuration
 * has been configured. The usage through {@link Condition} requires an existing application context while the
 * {@link TestExecutionListener} usage detects the activated profiles early to avoid expensive application context
 * startup if the condition does not match.
 *
 * @author Mark Paluch
 */
@Order(Integer.MIN_VALUE)
class DatabaseTypeCondition implements Condition, TestExecutionListener {

	@Override
	public void prepareTestInstance(TestContext testContext) {
		evaluate(testContext.getTestClass(), new StandardEnvironment(), true);
	}

	@Override
	public void beforeTestMethod(TestContext testContext) {
		evaluate(testContext.getTestMethod(),
				(ConfigurableEnvironment) testContext.getApplicationContext().getEnvironment(), false);
	}

	private static void evaluate(AnnotatedElement element, ConfigurableEnvironment environment,
			boolean enabledByDefault) {

		Optional<DatabaseType> databaseType = AnnotationUtils.findAnnotation(element, ConditionalOnDatabase.class)
				.map(ConditionalOnDatabase::value);

		if (databaseType.isEmpty()) {
			databaseType = AnnotationUtils.findAnnotation(element, EnabledOnDatabase.class).map(EnabledOnDatabase::value);
		}

		if (databaseType.isPresent()) {

			DatabaseType type = databaseType.get();

			if (enabledByDefault) {
				EnabledOnDatabaseCustomizer.customizeEnvironment(environment, type);
			}

			assumeThat(environment.getActiveProfiles()).as("Enabled profiles").contains(type.getProfile());
		}
	}

	@Override
	public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {

		MultiValueMap<String, Object> attrs = metadata.getAllAnnotationAttributes(ConditionalOnDatabase.class.getName());
		if (attrs != null) {
			for (Object value : attrs.get("value")) {

				DatabaseType type = (DatabaseType) value;

				if (context.getEnvironment().matchesProfiles(type.getProfile())) {
					return true;
				}
			}

			return false;
		}

		return true;
	}

}
