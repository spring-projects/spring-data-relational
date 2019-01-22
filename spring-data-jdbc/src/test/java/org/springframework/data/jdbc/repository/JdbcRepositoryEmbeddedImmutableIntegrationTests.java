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

import lombok.Value;
import lombok.experimental.Wither;
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
 * Very simple use cases for creation and usage of JdbcRepositories with {@link Embedded} annotation in Entities.
 *
 * @author Bastian Wilhelm
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryEmbeddedImmutableIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryEmbeddedImmutableIntegrationTests.class;
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
	}

	@Test // DATAJDBC-111
	public void saveAndLoadAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getPrefixedEmbeddable().getAttr1()).isEqualTo(entity.getPrefixedEmbeddable().getAttr1());
			assertThat(it.getPrefixedEmbeddable().getAttr2()).isEqualTo(entity.getPrefixedEmbeddable().getAttr2());
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

		entity.setPrefixedEmbeddable(entity.getPrefixedEmbeddable().withAttr2("something else"));
		DummyEntity saved = repository.save(entity);

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getPrefixedEmbeddable().getAttr2()).isEqualTo(saved.getPrefixedEmbeddable().getAttr2());
		});
	}

	@Test // DATAJDBC-111
	public void updateMany() {

		DummyEntity entity = repository.save(createDummyEntity());
		DummyEntity other = repository.save(createDummyEntity());

		entity.setPrefixedEmbeddable(entity.getPrefixedEmbeddable().withAttr2("something else"));
		other.setPrefixedEmbeddable(entity.getPrefixedEmbeddable().withAttr2("others Name"));

		repository.saveAll(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(d -> d.getPrefixedEmbeddable().getAttr2()) //
				.containsExactlyInAnyOrder(entity.getPrefixedEmbeddable().getAttr2(), other.getPrefixedEmbeddable().getAttr2());
	}

	private static DummyEntity createDummyEntity() {
		DummyEntity entity = new DummyEntity();

		entity.setPrefixedEmbeddable(new Embeddable(1L, "test1"));

		return entity;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	static class DummyEntity {

		@Id Long id;

		@Embedded("prefix_") Embeddable prefixedEmbeddable;
	}

	@Value
	@Wither
	private static class Embeddable {
		Long attr1;
		String attr2;
	}
}
