/*
 * Copyright 2017-2018 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;

	@Test // DATAJDBC-95
	public void savesAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(JdbcTestUtils.countRowsInTableWhere((JdbcTemplate) template.getJdbcOperations(), "dummy_entity",
				"id_Prop = " + entity.getIdProp())).isEqualTo(1);
	}

	@Test // DATAJDBC-95
	public void saveAndLoadAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getIdProp())).hasValueSatisfying(it -> {

			assertThat(it.getIdProp()).isEqualTo(entity.getIdProp());
			assertThat(it.getName()).isEqualTo(entity.getName());
		});
	}

	@Test // DATAJDBC-97
	public void savesManyEntities() {

		DummyEntity entity = createDummyEntity();
		DummyEntity other = createDummyEntity();

		repository.saveAll(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder(entity.getIdProp(), other.getIdProp());
	}

	@Test // DATAJDBC-97
	public void existsReturnsTrueIffEntityExists() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.existsById(entity.getIdProp())).isTrue();
		assertThat(repository.existsById(entity.getIdProp() + 1)).isFalse();
	}

	@Test // DATAJDBC-97
	public void findAllFindsAllEntities() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all)//
				.extracting(DummyEntity::getIdProp)//
				.containsExactlyInAnyOrder(entity.getIdProp(), other.getIdProp());
	}

	@Test // DATAJDBC-97
	public void findAllFindsAllSpecifiedEntities() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		assertThat(repository.findAllById(asList(entity.getIdProp(), other.getIdProp())))//
				.extracting(DummyEntity::getIdProp)//
				.containsExactlyInAnyOrder(entity.getIdProp(), other.getIdProp());
	}

	@Test // DATAJDBC-97
	public void countsEntities() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		assertThat(repository.count()).isEqualTo(3L);
	}

	@Test // DATAJDBC-97
	public void deleteById() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.deleteById(two.getIdProp());

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder(one.getIdProp(), three.getIdProp());
	}

	@Test // DATAJDBC-97
	public void deleteByEntity() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.delete(one);

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder(two.getIdProp(), three.getIdProp());
	}

	@Test // DATAJDBC-97
	public void deleteByList() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.deleteAll(asList(one, three));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder(two.getIdProp());
	}

	@Test // DATAJDBC-97
	public void deleteAll() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		assertThat(repository.findAll()).isNotEmpty();

		repository.deleteAll();

		assertThat(repository.findAll()).isEmpty();
	}

	@Test // DATAJDBC-98
	public void update() {

		DummyEntity entity = repository.save(createDummyEntity());

		entity.setName("something else");
		DummyEntity saved = repository.save(entity);

		assertThat(repository.findById(entity.getIdProp())).hasValueSatisfying(it -> {
			assertThat(it.getName()).isEqualTo(saved.getName());
		});
	}

	@Test // DATAJDBC-98
	public void updateMany() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		entity.setName("something else");
		other.setName("others Name");

		repository.saveAll(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getName) //
				.containsExactlyInAnyOrder(entity.getName(), other.getName());
	}

	@Test // DATAJDBC-112
	public void findByIdReturnsEmptyWhenNoneFound() {

		// NOT saving anything, so DB is empty

		assertThat(repository.findById(-1L)).isEmpty();
	}

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");
		return entity;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	static class DummyEntity {

		String name;
		@Id private Long idProp;
	}
}
