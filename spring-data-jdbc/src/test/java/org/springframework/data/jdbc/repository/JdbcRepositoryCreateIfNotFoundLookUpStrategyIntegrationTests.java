/*
 * Copyright 2022 the original author or authors.
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

/**
 * Test to verify that
 * <code>@EnableJdbcRepositories(queryLookupStrategy = QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND)</code> works as
 * intended. Tests based on logic from
 * {@link org.springframework.data.jdbc.repository.support.JdbcQueryLookupStrategy.CreateIfNotFoundQueryLookupStrategy}
 *
 * @author Diego Krupitza
 */
@ContextConfiguration
@Transactional
@ActiveProfiles("hsql")
@ExtendWith(SpringExtension.class)
class JdbcRepositoryCreateIfNotFoundLookUpStrategyIntegrationTests
		extends AbstractJdbcRepositoryLookUpStrategyIntegrationTests {

	@Test
	void declaredQueryShouldWork() {
		onesRepository.deleteAll();
		callDeclaredQuery("D", 2, "Diego", "Daniela");
	}

	@Test
	void derivedQueryShouldWork() {
		onesRepository.deleteAll();
		callDerivedQuery();
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			queryLookupStrategy = QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND,
			includeFilters = @ComponentScan.Filter(
					value = AbstractJdbcRepositoryLookUpStrategyIntegrationTests.OnesRepository.class,
					type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return AbstractJdbcRepositoryLookUpStrategyIntegrationTests.class;
		}
	}

}
