/*
 * Copyright 2019-2024 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

/**
 * Very simple use cases for creation and usage of JdbcRepositories with test {@link Embedded} annotation in Entities.
 *
 * @author Bastian Wilhelm
 */
@IntegrationTest
public class JdbcRepositoryEmbeddedNotInAggregateRootIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired Dialect dialect;

	private static DummyEntity createDummyEntity() {
		DummyEntity entity = new DummyEntity();

		entity.setTest("rootTest");

		final DummyEntity2 dummyEntity2 = new DummyEntity2();
		dummyEntity2.setTest("c1");

		final Embeddable embeddable = new Embeddable();
		embeddable.setAttr(1L);
		dummyEntity2.setEmbeddable(embeddable);

		entity.setDummyEntity2(dummyEntity2);

		return entity;
	}

	@Test // DATAJDBC-111
	public void savesAnEntity() throws SQLException {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(countRowsInTable("dummy_entity", entity.getId())).isEqualTo(1);
		assertThat(countRowsInTable("dummy_entity2", entity.getId())).isEqualTo(1);
	}

	private int countRowsInTable(String name, long idValue) {

		SqlIdentifier id = SqlIdentifier.quoted("ID");
		String whereClause = id.toSql(dialect.getIdentifierProcessing()) + " = " + idValue;

		return JdbcTestUtils.countRowsInTableWhere(template.getJdbcOperations(), name, whereClause);
	}

	@Test // DATAJDBC-111
	public void saveAndLoadAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getDummyEntity2().getTest()).isEqualTo(entity.getDummyEntity2().getTest());
			assertThat(it.getDummyEntity2().getEmbeddable().getAttr())
					.isEqualTo(entity.getDummyEntity2().getEmbeddable().getAttr());
		});
	}

	@Test // DATAJDBC-111
	public void findAllFindsAllEntities() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all)//
				.extracting(DummyEntity::getId)//
				.containsExactlyInAnyOrder(entity.getId(), other.getId());
	}

	@Test // DATAJDBC-111
	public void findByIdReturnsEmptyWhenNoneFound() {

		// NOT saving anything, so DB is empty
		assertThat(repository.findById(-1L)).isEmpty();
	}

	@Test // DATAJDBC-111
	public void update() {

		DummyEntity entity = repository.save(createDummyEntity());

		entity.getDummyEntity2().setTest("something else");
		entity.getDummyEntity2().getEmbeddable().setAttr(3L);
		DummyEntity saved = repository.save(entity);

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getDummyEntity2().getTest()).isEqualTo(saved.getDummyEntity2().getTest());
			assertThat(it.getDummyEntity2().getEmbeddable().getAttr())
					.isEqualTo(saved.getDummyEntity2().getEmbeddable().getAttr());
		});
	}

	@Test // DATAJDBC-111
	public void updateMany() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		entity.getDummyEntity2().setTest("something else");
		other.getDummyEntity2().setTest("others Name");

		entity.getDummyEntity2().getEmbeddable().setAttr(3L);
		other.getDummyEntity2().getEmbeddable().setAttr(5L);

		repository.saveAll(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(d -> d.getDummyEntity2().getTest()) //
				.containsExactlyInAnyOrder(entity.getDummyEntity2().getTest(), other.getDummyEntity2().getTest());

		assertThat(repository.findAll()) //
				.extracting(d -> d.getDummyEntity2().getEmbeddable().getAttr()) //
				.containsExactlyInAnyOrder(entity.getDummyEntity2().getEmbeddable().getAttr(),
						other.getDummyEntity2().getEmbeddable().getAttr());
	}

	@Test // DATAJDBC-111
	public void deleteById() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.deleteById(two.getId());

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(one.getId(), three.getId());
	}

	@Test // DATAJDBC-111
	public void deleteByEntity() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.delete(one);

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(two.getId(), three.getId());
	}

	@Test // DATAJDBC-111
	public void deleteByList() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.deleteAll(asList(one, three));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(two.getId());
	}

	@Test // DATAJDBC-111
	public void deleteAll() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		assertThat(repository.findAll()).isNotEmpty();

		repository.deleteAll();

		assertThat(repository.findAll()).isEmpty();
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		DummyEntityRepository dummyEntityRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(DummyEntityRepository.class);
		}

	}

	static class DummyEntity {
		@Column("ID")
		@Id
		Long id;

		String test;

		@Column("ID")
		DummyEntity2 dummyEntity2;

		public Long getId() {
			return this.id;
		}

		public String getTest() {
			return this.test;
		}

		public DummyEntity2 getDummyEntity2() {
			return this.dummyEntity2;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setTest(String test) {
			this.test = test;
		}

		public void setDummyEntity2(DummyEntity2 dummyEntity2) {
			this.dummyEntity2 = dummyEntity2;
		}
	}

	static class DummyEntity2 {
		@Column("ID")
		@Id
		Long id;

		String test;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_")
		Embeddable embeddable;

		public Long getId() {
			return this.id;
		}

		public String getTest() {
			return this.test;
		}

		public Embeddable getEmbeddable() {
			return this.embeddable;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setTest(String test) {
			this.test = test;
		}

		public void setEmbeddable(Embeddable embeddable) {
			this.embeddable = embeddable;
		}
	}

	static class Embeddable {
		Long attr;

		public Long getAttr() {
			return this.attr;
		}

		public void setAttr(Long attr) {
			this.attr = attr;
		}
	}
}
