/*
 * Copyright 2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.DatabaseProfileValueSource;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases of JdbcRepositories for Entities with schema.
 *
 * @author Myeonghyeon Lee
 */
@ContextConfiguration
@ProfileValueSourceConfiguration(DatabaseProfileValueSource.class)
@Transactional
public class JdbcRepositoryWithSchemaIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryWithSchemaIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		NamingStrategy namingStrategy() {
			return new NamingStrategy() {
				@Override
				public String getSchema() {
					return "test";
				}
			};
		}
	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired
	DummyEntityRepository repository;

	@Test // DATAJDBC-547
	public void deleteById() {

		DummyEntity entity = createDummyEntity();

		entity = repository.save(entity);

		repository.deleteById(entity.id);

		assertThat(repository.findById(entity.id)).isEmpty();
	}

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");
		return entity;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	static class DummyEntity {

		@Id private Long id;
		String name;
		List<Element> content = new ArrayList<>();

	}

	@RequiredArgsConstructor
	static class Element {

		@Id private Long id;
		String content;
	}

}
