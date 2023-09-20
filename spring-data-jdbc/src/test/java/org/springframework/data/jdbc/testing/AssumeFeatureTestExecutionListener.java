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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.context.ApplicationContext;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

/**
 * {@link TestExecutionListener} to evaluate {@link EnabledOnFeature} annotations.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class AssumeFeatureTestExecutionListener implements TestExecutionListener {

	@Override
	public void beforeTestMethod(TestContext testContext) {

		ApplicationContext applicationContext = testContext.getApplicationContext();
		TestDatabaseFeatures databaseFeatures = applicationContext.getBean(TestDatabaseFeatures.class);

		Set<Feature> requiredFeatures = new LinkedHashSet<>();

		EnabledOnFeature classAnnotation = testContext.getTestClass().getAnnotation(EnabledOnFeature.class);
		if (classAnnotation != null) {
			requiredFeatures.addAll(Arrays.asList(classAnnotation.value()));
		}

		EnabledOnFeature methodAnnotation = testContext.getTestMethod().getAnnotation(EnabledOnFeature.class);
		if (methodAnnotation != null) {
			requiredFeatures.addAll(Arrays.asList(methodAnnotation.value()));
		}

		for (TestDatabaseFeatures.Feature requiredFeature : requiredFeatures) {
			requiredFeature.test(databaseFeatures);
		}
	}
}
