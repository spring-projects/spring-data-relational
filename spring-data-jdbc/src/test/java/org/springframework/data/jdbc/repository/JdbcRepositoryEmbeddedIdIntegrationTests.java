/*
 * Copyright 2019-2020 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Simple JdbcRepository test has {@link Embedded} {@link Id}.
 *
 * @author Yunyoung LEE
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryEmbeddedIdIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryEmbeddedIdIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired DummyEntityRepository repository;

	@Test // DATAJDBC-352
	public void savesAnEntity() {

		final DummyEntity entity = createDummyEntity(1L);
		entity.createSubEntity(1L, "A");

		repository.save(entity);
		assertThat(repository.count()).isEqualTo(1L);
	}

	@Test // DATAJDBC-352
	public void saveAndLoadAnEntity() {

		final DummyEntity newEntity = createDummyEntity(1L);
		newEntity.createSubEntity(1L, "A");

		final DummyEntity entity = repository.save(newEntity);

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getId().getDummyId1()).isEqualTo(entity.getId().getDummyId1());
			assertThat(it.getId().getDummyId2()).isEqualTo(entity.getId().getDummyId2());
			assertThat(it.getDummyAttr()).isEqualTo(entity.getDummyAttr());
			assertThat(it.getSubEntities()).isEqualTo(entity.getSubEntities());
		});
	}

	@Test // DATAJDBC-352
	public void findAllFindsAllEntities() {

		DummyEntity entity = repository.save(createDummyEntity(1L));
		DummyEntity other = repository.save(createDummyEntity(2L));

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all)//
				.extracting(DummyEntity::getId)//
				.containsExactlyInAnyOrder(entity.getId(), other.getId());
	}

	@Test // DATAJDBC-352
	public void findByIdReturnsEmptyWhenNoneFound() {

		// NOT saving anything, so DB is empty
		assertThat(repository.findById(new DummyEntityId(-1L, -1L))).isEmpty();
	}

	@Test // DATAJDBC-352
	public void findAllById() {

		DummyEntity one = repository.save(createDummyEntity(1L));
		@SuppressWarnings("unused")
		DummyEntity two = repository.save(createDummyEntity(2L));
		DummyEntity three = repository.save(createDummyEntity(3L));

		Iterable<DummyEntity> entities = repository.findAllById(asList(three.getId(), one.getId()));

		assertThat(entities)//
				.extracting(DummyEntity::getId)//
				.containsExactlyInAnyOrder(three.getId(), one.getId());
	}

	@Test // DATAJDBC-352
	public void findByAttribute() {

		DummyEntity entity = repository.save(createDummyEntity(1L));

		assertThat(repository.findByDummyAttr(entity.getDummyAttr())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getId().getDummyId1()).isEqualTo(entity.getId().getDummyId1());
			assertThat(it.getId().getDummyId2()).isEqualTo(entity.getId().getDummyId2());
			assertThat(it.getDummyAttr()).isEqualTo(entity.getDummyAttr());
		});

	}

	@Test // DATAJDBC-352
	public void update() {

		DummyEntity entity = repository.save(createDummyEntity(1L));

		entity.setDummyAttr("New Attr");
		entity.setNew(false);
		DummyEntity saved = repository.save(entity);

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getDummyAttr()).isEqualTo(saved.getDummyAttr());
		});
	}

	@Test // DATAJDBC-352
	public void updateMany() {

		DummyEntity entity = repository.save(createDummyEntity(3L));
		DummyEntity other = repository.save(createDummyEntity(1L));

		entity.setDummyAttr("entity attr");
		entity.setNew(false);

		other.setDummyAttr("other attr");
		other.setNew(false);

		repository.saveAll(asList(entity, other));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getDummyAttr) //
				.containsExactlyInAnyOrder(entity.getDummyAttr(), other.getDummyAttr());
	}

	@Test // DATAJDBC-352
	public void deleteById() {

		DummyEntity one = repository.save(createDummyEntity(1L));
		DummyEntity two = repository.save(createDummyEntity(2L));
		DummyEntity three = repository.save(createDummyEntity(3L));

		repository.deleteById(two.getId());

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(one.getId(), three.getId());
	}

	@Test // DATAJDBC-352
	public void deleteByEntity() {
		DummyEntity one = repository.save(createDummyEntity(1L));
		DummyEntity two = repository.save(createDummyEntity(2L));
		DummyEntity three = repository.save(createDummyEntity(3L));

		repository.delete(one);

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(two.getId(), three.getId());
	}

	@Test // DATAJDBC-352
	public void deleteByList() {

		DummyEntity one = repository.save(createDummyEntity(1L));
		DummyEntity two = repository.save(createDummyEntity(2L));
		DummyEntity three = repository.save(createDummyEntity(3L));

		repository.deleteAll(asList(one, three));

		assertThat(repository.findAll()) //
				.extracting(DummyEntity::getId) //
				.containsExactlyInAnyOrder(two.getId());
	}

	@Test // DATAJDBC-352
	public void deleteAll() {

		repository.save(createDummyEntity(1L));
		repository.save(createDummyEntity(2L));
		repository.save(createDummyEntity(3L));

		assertThat(repository.findAll()).isNotEmpty();

		repository.deleteAll();

		assertThat(repository.findAll()).isEmpty();
	}

	private static DummyEntity createDummyEntity(Long id) {
		DummyEntity entity = new DummyEntity();

		entity.setId(new DummyEntityId(id, id));
		entity.setDummyAttr("attr");
		entity.setNew(true);

		return entity;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, DummyEntityId> {
		Optional<DummyEntity> findByDummyAttr(String attr);
	}

	@Data
	static class DummyEntity implements Persistable<DummyEntityId> {
		@Id @Embedded.Nullable DummyEntityId id;

		String dummyAttr;

		@MappedCollection(idColumns = { "DUMMY_ID1", "DUMMY_ID2" }) Set<SubEntity> subEntities;

		@Transient boolean isNew;

		SubEntity createSubEntity(Long subId, String subAttr) {
			final SubEntity subEntity = new SubEntity();
			subEntity.setId(new SubEntityId(id, subId));
			subEntity.setSubAttr(subAttr);

			if (subEntities == null) {
				subEntities = new HashSet<>();
			}

			subEntities.add(subEntity);
			return subEntity;
		}
	}

	@Data
	@AllArgsConstructor
	static class DummyEntityId {
		Long dummyId1;
		Long dummyId2;
	}

	@Data
	static class SubEntity {
		@Id @Embedded.Nullable SubEntityId id;
		String subAttr;
	}

	@Data
	@AllArgsConstructor
	static class SubEntityId {
		@Embedded.Nullable DummyEntityId dummyEntityId;
		Long subId;
	}

}
