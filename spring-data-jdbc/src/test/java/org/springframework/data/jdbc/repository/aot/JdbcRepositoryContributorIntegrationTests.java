/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for AOT processing.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = JdbcRepositoryContributorIntegrationTests.JdbcRepositoryContributorConfiguration.class)
@IntegrationTest
@EnabledOnDatabase(DatabaseType.H2)
class JdbcRepositoryContributorIntegrationTests {

	@Autowired UserRepository fragment;
	@Autowired JdbcAggregateOperations operations;

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = String.class) })
	@Import(TestConfiguration.class)
	static class JdbcRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public JdbcRepositoryContributorConfiguration() {
			super(UserRepository.class, JdbcH2Dialect.INSTANCE, JdbcRepositoryContributorConfiguration.class);
		}

		@Bean
		TestClass testClass() {
			return TestClass.of(JdbcRepositoryContributorIntegrationTests.class);
		}

	}

	@BeforeEach
	void beforeEach() {

		operations.deleteAll(User.class);

		User user = new User();
		user.setFirstname("Walter");
		user.setAge(52);

		operations.insert(user);
	}

	@Test
	void shouldFindByFirstname() {

		User walter = fragment.findByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test
	void shouldFindAnnotatedByFirstname() {

		User walter = fragment.findByFirstnameAnnotated("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

}
