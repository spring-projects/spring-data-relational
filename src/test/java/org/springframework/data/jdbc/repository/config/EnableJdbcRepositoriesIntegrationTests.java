/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.config;

import static org.junit.Assert.assertNotNull;

import lombok.Data;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositoriesIntegrationTests.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class EnableJdbcRepositoriesIntegrationTests {

	@Autowired DummyRepository repository;

	@Test // DATAJDBC-100
	public void repositoryGetsPickedUp() {

		assertNotNull(repository);

		Iterable<DummyEntity> all = repository.findAll();

		assertNotNull(all);
	}

	interface DummyRepository extends CrudRepository<DummyEntity, Long> {

	}

	@Data
	static class DummyEntity {
		@Id private Long id;
	}

	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return EnableJdbcRepositoriesIntegrationTests.class;
		}
	}
}
