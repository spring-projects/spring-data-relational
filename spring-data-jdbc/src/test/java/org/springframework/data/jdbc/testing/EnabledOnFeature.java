/*
 * Copyright 2020-2024 the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code @RequiredFeature} is used to express that the annotated test class or test method is only <em>enabled</em> on
 * one or more specified Spring Data JDBC {@link org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature
 * features} are supported by the underlying database.
 * <p>
 * When applied at the class level, all test methods within that class will be enabled if they support all database
 * features.
 * <p>
 * If a test method is disabled via this annotation, that does not prevent the test class from being instantiated.
 * Rather, it prevents the execution of the test method and method-level lifecycle callbacks such as {@code @BeforeEach}
 * methods, {@code @AfterEach} methods, and corresponding extension APIs. When annotated on method and class level, all
 * annotated features must match to run a test.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@Documented
public @interface EnabledOnFeature {

	/**
	 * Databases features on which the annotated class or method should be enabled.
	 *
	 * @see TestDatabaseFeatures.Feature
	 */
	TestDatabaseFeatures.Feature[] value();
}
