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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.JdbcRepositoryIdGenerationIntegrationTests.TestConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Testing special cases for id generation with {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class JdbcRepositoryIdGenerationIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;

	@Autowired ReadOnlyIdEntityRepository readOnlyIdrepository;

	@Autowired PrimitiveIdEntityRepository primitiveIdRepository;

	@Test // DATAJDBC-98
	public void idWithoutSetterGetsSet() {

		ReadOnlyIdEntity entity1 = new ReadOnlyIdEntity(null);
		entity1.setName("Entity Name");
		ReadOnlyIdEntity entity = entity1;
		entity = readOnlyIdrepository.save(entity);

		assertThat(entity.getId()).isNotNull();

		ReadOnlyIdEntity reloadedEntity = readOnlyIdrepository.findOne(entity.getId());

		assertEquals(entity.getId(), reloadedEntity.getId());
		assertEquals(entity.getName(), reloadedEntity.getName());
	}

	@Test // DATAJDBC-98
	public void primitiveIdGetsSet() {

		PrimitiveIdEntity entity = new PrimitiveIdEntity(0);
		entity.setName("Entity Name");
		entity = primitiveIdRepository.save(entity);

		assertThat(entity.getId()).isNotNull();

		PrimitiveIdEntity reloadedEntity = primitiveIdRepository.findOne(entity.getId());

		assertEquals(entity.getId(), reloadedEntity.getId());
		assertEquals(entity.getName(), reloadedEntity.getName());
	}

	private interface ReadOnlyIdEntityRepository extends CrudRepository<ReadOnlyIdEntity, Long> {

	}

	@Data
	static class ReadOnlyIdEntity {

		@Id private final Long id;
		String name;
	}

	private interface PrimitiveIdEntityRepository extends CrudRepository<PrimitiveIdEntity, Long> {

	}

	@Data
	static class PrimitiveIdEntity {

		@Id private final long id;
		String name;
	}

	@Configuration
	static class TestConfiguration {

		@Bean
		EmbeddedDatabase dataSource() {

			System.out.println(" creating datasource");
			return new EmbeddedDatabaseBuilder() //
					.generateUniqueName(true) //
					.setType(EmbeddedDatabaseType.HSQL) //
					.setScriptEncoding("UTF-8") //
					.ignoreFailedDrops(true) //
					.addScript("org.springframework.data.jdbc.repository/jdbc-repository-id-generation-integration-tests.sql")
					.build();
		}

		@Bean
		NamedParameterJdbcTemplate template(EmbeddedDatabase db) {
			return new NamedParameterJdbcTemplate(db);
		}

		@Bean
		ReadOnlyIdEntityRepository readOnlyIdRepository(EmbeddedDatabase db) {

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
