/*
 * Copyright 2017-2019 the original author or authors.
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
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories with test {@link Embedded} annotation in Entities.
 *
 * @author Bastian Wilhelm
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryEmbeddedNotInAggregateRootIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryEmbeddedNotInAggregateRootIntegrationTests.class;
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

	@Test // DATAJDBC-111
	public void savesAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(JdbcTestUtils.countRowsInTableWhere((JdbcTemplate) template.getJdbcOperations(), "dummy_entity",
				"id = " + entity.getId())).isEqualTo(1);

		assertThat(JdbcTestUtils.countRowsInTableWhere((JdbcTemplate) template.getJdbcOperations(), "dummy_entity2",
				"id = " + entity.getId())).isEqualTo(1);
	}

	@Test // DATAJDBC-111
	public void saveAndLoadAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getDummyEntity2().getTest()).isEqualTo(entity.getDummyEntity2().getTest());
			assertThat(it.getDummyEntity2().getEmbeddable().getAttr()).isEqualTo(entity.getDummyEntity2().getEmbeddable().getAttr());
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
	public void update() {

		DummyEntity entity = repository.save(createDummyEntity());

		entity.getDummyEntity2().setTest("something else");
		entity.getDummyEntity2().getEmbeddable().setAttr(3L);
		DummyEntity saved = repository.save(entity);

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getDummyEntity2().getTest()).isEqualTo(saved.getDummyEntity2().getTest());
			assertThat(it.getDummyEntity2().getEmbeddable().getAttr()).isEqualTo(saved.getDummyEntity2().getEmbeddable().getAttr());
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
				.containsExactlyInAnyOrder(entity.getDummyEntity2().getEmbeddable().getAttr(), other.getDummyEntity2().getEmbeddable().getAttr());
	}

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

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	static class DummyEntity {
		@Id Long id;

		String test;

		@Column("id")
		DummyEntity2 dummyEntity2;
	}

	@Data
	static class DummyEntity2 {
		@Id Long id;

		String test;

		@Embedded("prefix_")
		Embeddable embeddable;
	}

	@Data
	static class Embeddable {
		Long attr;
	}
}
