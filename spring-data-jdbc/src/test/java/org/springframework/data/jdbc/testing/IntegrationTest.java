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

import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.jdbc.testing.EnabledOnDatabaseCustomizer.EnabledOnDatabaseCustomizerFactory;
import org.springframework.data.jdbc.testing.TestClassCustomizer.TestClassCustomizerFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextCustomizerFactories;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

/**
 * {@code @IntegrationTest} is a <em>composed annotation</em> that combines
 * {@link ExtendWith @ExtendWith(SpringExtension.class)} from JUnit Jupiter with
 * {@link ContextConfiguration @ContextConfiguration} and {@link TestExecutionListeners @TestExecutionListeners} from
 * the <em>Spring TestContext Framework</em> enabling transaction management.
 * <p>
 * Integration tests use the Spring Context and a potential profile to create an environment for tests to run against.
 * As integration tests require a specific set of infrastructure components, test classes and configuration components
 * can be annotated with {@link EnabledOnDatabase @EnabledOnDatabase} or
 * {@link ConditionalOnDatabase @ConditionalOnDatabase} to enable and restrict or only restrict configuration on which
 * tests are ran.
 *
 * @author Mark Paluch
 * @see ConditionalOnDatabase
 * @see EnabledOnDatabase
 */
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
// required twice as the annotation lookup doesn't merge multiple occurences of the same annotation
@ContextCustomizerFactories(value = { TestClassCustomizerFactory.class, EnabledOnDatabaseCustomizerFactory.class })
@ActiveProfiles(resolver = CombiningActiveProfileResolver.class)
@ExtendWith(SpringExtension.class)
@Transactional
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface IntegrationTest {

}
