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

import static org.junit.Assert.*;

import java.sql.SQLException;
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

	private DummyEntity entity = createDummyEntity();

	@After
	public void after() {
		db.shutdown();
	}


	@Test
	public void canSaveAnEntity() throws SQLException {

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
	public void canSaveAndLoadAnEntity() throws SQLException {

		entity = repository.save(entity);

		DummyEntity reloadedEntity = repository.findOne(entity.getId());

		assertEquals(
				entity.getId(),
				reloadedEntity.getId());
		assertEquals(
				entity.getName(),
				reloadedEntity.getName());
	}

	private static DummyEntityRepository createRepository(EmbeddedDatabase db) {
		return new JdbcRepositoryFactory(db).getRepository(DummyEntityRepository.class);
	}


	private static DummyEntity createDummyEntity() {
		DummyEntity entity = new DummyEntity();
		entity.setId(23L);
		entity.setName("Entity Name");
		return entity;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

	@Data
	private static class DummyEntity {

		@Id
		Long id;
		String name;
	}
}
