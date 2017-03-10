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
import static org.mockito.Mockito.*;

import lombok.Data;

import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.JdbcRepositoryIntegrationTests.TestConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
@Transactional
public class JdbcRepositoryIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;

	@Autowired DummyEntityRepository repository;

	private DummyEntity entity = createDummyEntity();

	@Test // DATAJDBC-95
	public void savesAnEntity() {

		entity = repository.save(entity);

		int count = template.queryForObject( //
				"SELECT count(*) FROM dummyentity WHERE idProp = :id", //
				new MapSqlParameterSource("id", entity.getIdProp()), //
				Integer.class //
		);

		assertEquals(1, count);
	}

	@Test // DATAJDBC-95
	public void saveAndLoadAnEntity() {

		entity = repository.save(entity);

		DummyEntity reloadedEntity = repository.findOne(entity.getIdProp());

		assertEquals(entity.getIdProp(), reloadedEntity.getIdProp());
		assertEquals(entity.getName(), reloadedEntity.getName());
	}

	@Test // DATAJDBC-97
	public void savesManyEntities() {

		DummyEntity other = createDummyEntity();

		repository.save(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder( //
						entity.getIdProp(), other.getIdProp() //
		);
	}

	@Test // DATAJDBC-97
	public void existsReturnsTrueIffEntityExists() {

		entity = repository.save(entity);

		assertTrue(repository.exists(entity.getIdProp()));
		assertFalse(repository.exists(entity.getIdProp() + 1));
	}

	@Test // DATAJDBC-97
	public void findAllFindsAllEntities() {

		DummyEntity other = createDummyEntity();

		other = repository.save(other);
		entity = repository.save(entity);

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all).extracting("idProp").containsExactlyInAnyOrder(entity.getIdProp(), other.getIdProp());
	}

	@Test // DATAJDBC-97
	public void findAllFindsAllSpecifiedEntities() {

		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());
		entity = repository.save(entity);

		Iterable<DummyEntity> all = repository.findAll(asList(entity.getIdProp(), three.getIdProp()));

		assertThat(all).extracting("idProp").containsExactlyInAnyOrder(entity.getIdProp(), three.getIdProp());
	}

	@Test // DATAJDBC-97
	public void countsEntities() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());
		repository.save(entity);

		assertThat(repository.count()).isEqualTo(3L);
	}

	@Test // DATAJDBC-97
	public void deleteById() {

		entity = repository.save(entity);
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.delete(two.getIdProp());

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder( //
						entity.getIdProp(), three.getIdProp() //
		);
	}

	@Test // DATAJDBC-97
	public void deleteByEntity() {

		entity = repository.save(entity);
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.delete(entity);

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getIdProp) //
				.containsExactlyInAnyOrder( //
						two.getIdProp(), three.getIdProp() //
		);
	}

	@Test // DATAJDBC-97
	public void deleteByList() {

		repository.save(entity);
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.delete(asList(entity, three));

		assertThat(repository.findAll()).extracting(DummyEntity::getIdProp).containsExactlyInAnyOrder(two.getIdProp());
	}

	@Test // DATAJDBC-97
	public void deleteAll() {

		repository.save(entity);
		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		repository.deleteAll();

		assertThat(repository.findAll()).isEmpty();
	}

	@Test // DATAJDBC-98
	public void update() {

		entity = repository.save(entity);

		entity.setName("something else");

		entity = repository.save(entity);

		DummyEntity reloaded = repository.findOne(entity.getIdProp());

		assertThat(reloaded.getName()).isEqualTo(entity.getName());
	}

	@Test // DATAJDBC-98
	public void updateMany() {

		entity = repository.save(entity);
		DummyEntity other = repository.save(createDummyEntity());

		entity.setName("something else");
		other.setName("others Name");

		repository.save(asList(entity, other));

		assertThat(repository.findAll()).extracting(DummyEntity::getName).containsExactlyInAnyOrder(entity.getName(),
				other.getName());
	}

	private static DummyEntityRepository createRepository(EmbeddedDatabase db) {

		return new JdbcRepositoryFactory(new NamedParameterJdbcTemplate(db), mock(ApplicationEventPublisher.class))
				.getRepository(DummyEntityRepository.class);
	}

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");
		return entity;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

	@Data
	static class DummyEntity {

		@Id private Long idProp;
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
					.addScript("org.springframework.data.jdbc.repository/jdbc-repository-integration-tests.sql") //
					.build();
		}

		@Bean
		NamedParameterJdbcTemplate template(EmbeddedDatabase db) {
			return new NamedParameterJdbcTemplate(db);
		}

		@Bean
		DummyEntityRepository readOnlyIdRepository(NamedParameterJdbcTemplate template) {

			return new JdbcRepositoryFactory(template, mock(ApplicationEventPublisher.class))
					.getRepository(DummyEntityRepository.class);
		}

		@Bean
		PlatformTransactionManager transactionManager(DataSource db) {
			return new DataSourceTransactionManager(db);
		}

	}
}
