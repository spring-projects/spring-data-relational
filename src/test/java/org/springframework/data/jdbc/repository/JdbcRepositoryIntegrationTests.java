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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import lombok.Data;

/**
 * very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryIntegrationTests {

	private final EmbeddedDatabase db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(EmbeddedDatabaseType.HSQL)
			.setScriptEncoding("UTF-8")
			.ignoreFailedDrops(true)
			.addScript("org.springframework.data.jdbc.repository/createTable.sql")
			.build();

	private final NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(db);

	private final DummyEntityRepository repository = createRepository(db);

	private DummyEntity entity = createDummyEntity(23L);

	@After
	public void after() {
		db.shutdown();
	}


	@Test
	public void canSaveAnEntity() {

		entity = repository.save(entity);

		int count = template.queryForObject(
				"SELECT count(*) FROM dummyentity WHERE id = :id",
				new MapSqlParameterSource("id", entity.getId()),
				Integer.class);

		assertEquals(
				1,
				count);
	}

	@Test
	public void canSaveAndLoadAnEntity() {

		entity = repository.save(entity);

		DummyEntity reloadedEntity = repository.findOne(entity.getId());

		assertEquals(
				entity.getId(),
				reloadedEntity.getId());
		assertEquals(
				entity.getName(),
				reloadedEntity.getName());
	}

	@Test
	public void canSaveAndLoadAnEntityWithDatabaseBasedIdGeneration() {

		entity = createDummyEntity(null);

		entity = repository.save(entity);

		assertThat(entity).isNotNull();

		DummyEntity reloadedEntity = repository.findOne(entity.getId());

		assertEquals(
				entity.getId(),
				reloadedEntity.getId());
		assertEquals(
				entity.getName(),
				reloadedEntity.getName());
	}


	@Test
	public void saveMany() {

		DummyEntity other = createDummyEntity(24L);

		repository.save(asList(entity, other));

		assertThat(repository.findAll()).extracting(DummyEntity::getId).containsExactlyInAnyOrder(23L, 24L);
	}

	@Test
	public void saveManyWithIdGeneration() {

		DummyEntity one = createDummyEntity(null);
		DummyEntity two = createDummyEntity(null);

		Iterable<DummyEntity> entities = repository.save(asList(one, two));

		assertThat(entities).allMatch(e -> e.getId() != null);

		assertThat(repository.findAll())
				.extracting(DummyEntity::getId)
				.containsExactlyInAnyOrder(new Long[]{one.getId(), two.getId()});
	}

	@Test
	public void existsReturnsTrueIffEntityExists() {

		entity = repository.save(entity);

		assertTrue(repository.exists(entity.getId()));
		assertFalse(repository.exists(entity.getId() + 1));
	}

	@Test
	public void findAllFindsAllEntities() {

		DummyEntity other = createDummyEntity(24L);

		other = repository.save(other);
		entity = repository.save(entity);

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all).extracting("id").containsExactlyInAnyOrder(entity.getId(), other.getId());
	}

	@Test
	public void findAllFindsAllSpecifiedEntities() {

		repository.save(createDummyEntity(24L));
		DummyEntity other = repository.save(createDummyEntity(25L));
		entity = repository.save(entity);

		Iterable<DummyEntity> all = repository.findAll(asList(entity.getId(), other.getId()));

		assertThat(all).extracting("id").containsExactlyInAnyOrder(entity.getId(), other.getId());
	}

	@Test
	public void count() {

		repository.save(createDummyEntity(24L));
		repository.save(createDummyEntity(25L));
		repository.save(entity);

		assertThat(repository.count()).isEqualTo(3L);
	}

	@Test
	public void deleteById() {

		repository.save(createDummyEntity(24L));
		repository.save(createDummyEntity(25L));
		repository.save(entity);

		repository.delete(24L);

		assertThat(repository.findAll()).extracting(DummyEntity::getId).containsExactlyInAnyOrder(23L, 25L);
	}

	@Test
	public void deleteByEntity() {

		repository.save(createDummyEntity(24L));
		repository.save(createDummyEntity(25L));
		repository.save(entity);

		repository.delete(entity);

		assertThat(repository.findAll()).extracting(DummyEntity::getId).containsExactlyInAnyOrder(24L, 25L);
	}


	@Test
	public void deleteByList() {

		repository.save(entity);
		repository.save(createDummyEntity(24L));
		DummyEntity other = repository.save(createDummyEntity(25L));

		repository.delete(asList(entity, other));

		assertThat(repository.findAll()).extracting(DummyEntity::getId).containsExactlyInAnyOrder(24L);
	}

	@Test
	public void deleteAll() {

		repository.save(entity);
		repository.save(createDummyEntity(24L));
		repository.save(createDummyEntity(25L));

		repository.deleteAll();

		assertThat(repository.findAll()).isEmpty();
	}


	private static DummyEntityRepository createRepository(EmbeddedDatabase db) {
		return new JdbcRepositoryFactory(db).getRepository(DummyEntityRepository.class);
	}


	private static DummyEntity createDummyEntity(Long id) {

		DummyEntity entity = new DummyEntity();
		entity.setId(id);
		entity.setName("Entity Name");
		return entity;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

	// needs to be public in order for the Hamcrest property matcher to work.
	@Data
	public static class DummyEntity {

		@Id
		Long id;
		String name;
	}
}
