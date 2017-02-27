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

import org.junit.After;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import lombok.Data;

/**
 * testing special cases for Id generation with JdbcRepositories.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryIdGenerationIntegrationTests {

	private final EmbeddedDatabase db = new EmbeddedDatabaseBuilder()
			.generateUniqueName(true)
			.setType(EmbeddedDatabaseType.HSQL)
			.setScriptEncoding("UTF-8")
			.ignoreFailedDrops(true)
			.addScript("org.springframework.data.jdbc.repository/jdbc-repository-id-generation-integration-tests.sql")
			.build();

	private final NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(db);

	private final ReadOnlyIdEntityRepository repository = createRepository(db);

	private ReadOnlyIdEntity entity = createDummyEntity();

	@After
	public void after() {
		db.shutdown();
	}

	@Test
	public void idWithoutSetterGetsSet() {

		entity = repository.save(entity);

		assertThat(entity.getId()).isNotNull();

		ReadOnlyIdEntity reloadedEntity = repository.findOne(entity.getId());

		assertEquals(
				entity.getId(),
				reloadedEntity.getId());
		assertEquals(
				entity.getName(),
				reloadedEntity.getName());
	}

	@Test
	public void primitiveIdGetsSet() {

		entity = repository.save(entity);

		assertThat(entity.getId()).isNotNull();

		ReadOnlyIdEntity reloadedEntity = repository.findOne(entity.getId());

		assertEquals(
				entity.getId(),
				reloadedEntity.getId());
		assertEquals(
				entity.getName(),
				reloadedEntity.getName());
	}


	private static ReadOnlyIdEntityRepository createRepository(EmbeddedDatabase db) {
		return new JdbcRepositoryFactory(db).getRepository(ReadOnlyIdEntityRepository.class);
	}


	private static ReadOnlyIdEntity createDummyEntity() {

		ReadOnlyIdEntity entity = new ReadOnlyIdEntity(null);
		entity.setName("Entity Name");
		return entity;
	}

	private interface ReadOnlyIdEntityRepository extends CrudRepository<ReadOnlyIdEntity, Long> {

	}

	@Data
	static class ReadOnlyIdEntity {

		@Id
		private final Long id;
		String name;
	}

	private interface PrimitiveIdEntityRepository extends CrudRepository<PrimitiveIdEntity, Long> {

	}

	@Data
	static class PrimitiveIdEntity {

		@Id
		private final Long id;
		String name;
	}
}
