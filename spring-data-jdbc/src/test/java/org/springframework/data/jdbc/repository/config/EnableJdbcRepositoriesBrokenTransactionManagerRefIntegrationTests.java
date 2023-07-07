/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Id;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(
		classes = EnableJdbcRepositoriesBrokenTransactionManagerRefIntegrationTests.TestConfiguration.class)
public class EnableJdbcRepositoriesBrokenTransactionManagerRefIntegrationTests {

	@Autowired DummyRepository repository;

	@Test // DATAJDBC-622
	public void missingTransactionManagerCausesException() {
		assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> repository.findAll());
	}

	interface DummyRepository extends CrudRepository<DummyEntity, Long> {

	}

	static class DummyEntity {
		@Id private Long id;

		public Long getId() {
			return this.id;
		}

		public void setId(Long id) {
			this.id = id;
		}
	}

	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class),
			transactionManagerRef = "no-such-transaction-manager")
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return EnableJdbcRepositoriesBrokenTransactionManagerRefIntegrationTests.class;
		}
	}
}
