/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.testing;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.*;
import static org.junit.platform.commons.util.AnnotationUtils.*;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.util.ClassUtils;

/**
 * {@link ExecutionCondition} for {@link EnabledOnClass @EnabledOnClass}.
 *
 * @author Mark Paluch
 * @see EnabledOnClass
 */
class EnabledOnClassCondition implements ExecutionCondition {

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		return findAnnotation(context.getElement(), EnabledOnClass.class) //
				.map(annotation -> isEnabled(annotation)
						? enabled(String.format("Class '%s' found on the class path.", annotation.value()))
						: disabled(String.format("Class '%s' not found on the class path.", annotation.value()))) //
				.orElseGet(this::enabledByDefault);
	}

	private boolean isEnabled(EnabledOnClass annotation) {
		return ClassUtils.isPresent(annotation.value(), EnabledOnClassCondition.class.getClassLoader());
	}

	private ConditionEvaluationResult enabledByDefault() {
		return enabled("@EnabledOnClass is not present");
	}

}
