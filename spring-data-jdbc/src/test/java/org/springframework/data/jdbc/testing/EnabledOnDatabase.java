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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.env.Environment;
import org.springframework.data.jdbc.testing.EnabledOnDatabaseCustomizer.EnabledOnDatabaseCustomizerFactory;
import org.springframework.data.jdbc.testing.TestClassCustomizer.TestClassCustomizerFactory;
import org.springframework.test.context.ContextCustomizerFactories;

/**
 * Selects a database configuration on which the test class is enabled.
 * <p>
 * Using this annotation will enable the test configuration if no test environment is given. If a test environment is
 * configured through {@link Environment#getActiveProfiles()}, then the test class will be skipped if the environment
 * doesn't match the specified {@link DatabaseType}.
 * <p>
 * If a test method is disabled via this annotation, that does not prevent the test class from being instantiated.
 * Rather, it prevents the execution of the test method and method-level lifecycle callbacks such as {@code @BeforeEach}
 * methods, {@code @AfterEach} methods, and corresponding extension APIs. When annotated on method and class level, all
 * annotated features must match to run a test.
 *
 * @author Mark Paluch
 * @see DatabaseTypeCondition
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
// required twice as the annotation lookup doesn't merge multiple occurences of the same annotation
@ContextCustomizerFactories(value = { TestClassCustomizerFactory.class, EnabledOnDatabaseCustomizerFactory.class })
@Documented
@Inherited
public @interface EnabledOnDatabase {

	/**
	 * Database type on which the annotated class should be enabled.
	 */
	DatabaseType value();
}
