/*
 * Copyright 2026-present the original author or authors.
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

import java.lang.reflect.AnnotatedElement;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StringUtils;

/**
 * JUnit 5 {@link ExecutionCondition} evaluating {@link EnabledOnDatabase} and {@link DisabledOnDatabase} annotations.
 * <p>
 * Requires {@link SpringExtension} to be present. The condition evaluates by default to
 * {@link ConditionEvaluationResult#enabled(String)} ), also in case of absence of the Spring context to prevent early
 * Spring initialization.
 *
 * @author Mark Paluch
 */
class OnDatabaseCondition implements ExecutionCondition {

	/**
	 * {@link ExtensionContext.Namespace} in which {@code TestContextManagers} are stored, keyed by test class.
	 */
	private static final ExtensionContext.Namespace TEST_CONTEXT_MANAGER_NAMESPACE = ExtensionContext.Namespace
			.create(SpringExtension.class);
	public static final ConditionEvaluationResult ENABLED_BY_DEFAULT = ConditionEvaluationResult
			.enabled("Enabled by default");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<AnnotatedElement> element = context.getElement();
		if (element.isEmpty()) {
			return ENABLED_BY_DEFAULT;
		}

		AnnotatedElement annotatedElement = element.get();
		MergedAnnotations annotations = MergedAnnotations.from(annotatedElement);
		MergedAnnotation<EnabledOnDatabase> enabled = annotations.get(EnabledOnDatabase.class);
		MergedAnnotation<DisabledOnDatabase> disabled = annotations.get(DisabledOnDatabase.class);

		if (!enabled.isPresent() && !disabled.isPresent()) {
			return ENABLED_BY_DEFAULT;
		}

		ExtensionContext.Store store = context.getRoot().getStore(TEST_CONTEXT_MANAGER_NAMESPACE);

		Class<?> testClass = context.getRequiredTestClass();
		TestContextManager testContextManager = store.get(testClass, TestContextManager.class);
		if (testContextManager == null) {
			return ConditionEvaluationResult.enabled("Enabled as there is no TestContextManager");
		}

		ApplicationContext applicationContext = SpringExtension.getApplicationContext(context);
		List<String> activeProfiles = List.of(applicationContext.getEnvironment().getActiveProfiles());

		if (enabled.isPresent()) {
			DatabaseType value = enabled.getEnum("value", DatabaseType.class);

			if (activeProfiles.contains(value.getProfile())
					|| (!containsDatabaseIdentifier(activeProfiles) && value == DatabaseType.HSQL)) {
				return ConditionEvaluationResult.enabled("Enabled by @EnabledOnDatabase(DatabaseType." + value + ")");
			}
		}

		if (disabled.isPresent()) {
			DatabaseType value = disabled.getEnum("value", DatabaseType.class);
			String reason = disabled.getString("disabledReason");
			String message = StringUtils.hasText(reason) ? reason
					: "Disabled by @DisabledOnDatabase(DatabaseType.%s)".formatted(value);

			if (activeProfiles.contains(value.getProfile())
					|| (!containsDatabaseIdentifier(activeProfiles) && value == DatabaseType.HSQL)) {
				return ConditionEvaluationResult.disabled(message);
			}
		}

		return ENABLED_BY_DEFAULT;
	}

	private static boolean containsDatabaseIdentifier(List<String> activeProfiles) {
		for (DatabaseType value : DatabaseType.values()) {
			if (activeProfiles.contains(value.getProfile())) {
				return true;
			}
		}

		return false;
	}

}
