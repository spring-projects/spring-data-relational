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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import lombok.Data;

import java.util.Optional;

import javax.sql.DataSource;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.JdbcRepositoryIdGenerationIntegrationTests.TestConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

/**
 * Testing special cases for id generation with {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 */
@ContextConfiguration(classes = TestConfiguration.class)
public class JdbcRepositoryIdGenerationIntegrationTests {

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired ReadOnlyIdEntityRepository readOnlyIdrepository;
	@Autowired PrimitiveIdEntityRepository primitiveIdRepository;

	@Test // DATAJDBC-98
	public void idWithoutSetterGetsSet() {

		ReadOnlyIdEntity entity = new ReadOnlyIdEntity(null);
		entity.setName("Entity Name");
		entity = readOnlyIdrepository.save(entity);

		assertThat(entity.getId()).isNotNull();

		Optional<ReadOnlyIdEntity> reloadedEntity = readOnlyIdrepository.findOne(entity.getId());

		assertTrue(reloadedEntity.isPresent());
		assertEquals(entity.getId(), reloadedEntity.get().getId());
		assertEquals(entity.getName(), reloadedEntity.get().getName());
	}

	@Test // DATAJDBC-98
	public void primitiveIdGetsSet() {

		PrimitiveIdEntity entity = new PrimitiveIdEntity(0);
		entity.setName("Entity Name");
		entity = primitiveIdRepository.save(entity);

		assertThat(entity.getId()).isNotNull();
		assertThat(entity.getId()).isNotEqualTo(0L);

		Optional<PrimitiveIdEntity> reloadedEntity = primitiveIdRepository.findOne(entity.getId());

		assertTrue(reloadedEntity.isPresent());
		assertEquals(entity.getId(), reloadedEntity.get().getId());
		assertEquals(entity.getName(), reloadedEntity.get().getName());
	}

	private interface ReadOnlyIdEntityRepository extends CrudRepository<ReadOnlyIdEntity, Long> {}

	private interface PrimitiveIdEntityRepository extends CrudRepository<PrimitiveIdEntity, Long> {}

	@Data
	static class ReadOnlyIdEntity {

		@Id private final Long id;
		String name;
	}

	@Data
	static class PrimitiveIdEntity {

		@Id private final long id;
		String name;
	}

	@Configuration
	@ComponentScan("org.springframework.data.jdbc.testing")
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIdGenerationIntegrationTests.class;
		}

		@Bean
		NamedParameterJdbcTemplate template(DataSource db) {
			return new NamedParameterJdbcTemplate(db);
		}

		@Bean
		ReadOnlyIdEntityRepository readOnlyIdRepository(DataSource db) {

			return new JdbcRepositoryFactory(new NamedParameterJdbcTemplate(db), mock(ApplicationEventPublisher.class))
					.getRepository(ReadOnlyIdEntityRepository.class);
		}

		@Bean
		PrimitiveIdEntityRepository primitiveIdRepository(NamedParameterJdbcTemplate template) {

			return new JdbcRepositoryFactory(template, mock(ApplicationEventPublisher.class))
					.getRepository(PrimitiveIdEntityRepository.class);
		}
	}
}
