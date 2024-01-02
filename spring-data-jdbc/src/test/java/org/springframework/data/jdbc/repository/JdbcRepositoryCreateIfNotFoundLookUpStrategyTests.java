/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.query.QueryLookupStrategy;

/**
 * Test to verify that
 * <code>@EnableJdbcRepositories(queryLookupStrategy = QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND)</code> works as
 * intended.
 *
 * @author Diego Krupitza
 * @author Jens Schauder
 */
@IntegrationTest
class JdbcRepositoryCreateIfNotFoundLookUpStrategyTests extends AbstractJdbcRepositoryLookUpStrategyTests {

	@Test // GH-1043
	void declaredQueryShouldWork() {
		onesRepository.deleteAll();
		callDeclaredQuery("D", 2, "Diego", "Daniela");
	}

	@Test // GH-1043
	void derivedQueryShouldWork() {
		onesRepository.deleteAll();
		callDerivedQuery();
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			queryLookupStrategy = QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND,
			includeFilters = @ComponentScan.Filter(value = AbstractJdbcRepositoryLookUpStrategyTests.OnesRepository.class,
					type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

		@Bean
		TestClass testClass() {
			// boostrap with a different SQL init script
			return TestClass.of(AbstractJdbcRepositoryLookUpStrategyTests.class);
		}
	}
}
