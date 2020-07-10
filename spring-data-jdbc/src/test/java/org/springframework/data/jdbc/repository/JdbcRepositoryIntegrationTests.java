/*
 * Copyright 2017-2020 the original author or authors.
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
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import lombok.Data;

import java.io.IOException;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.AssumeFeatureRule;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.event.AbstractRelationalEvent;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.Param;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Transactional
@TestExecutionListeners(value = AssumeFeatureRule.class, mergeMode = MERGE_WITH_DEFAULTS)
@RunWith(SpringRunner.class)
public class JdbcRepositoryIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired MyEventListener eventListener;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");

		return entity;
	}

	@Before
	public void before() {
		eventListener.events.clear();
	}

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

	@Test // DATAJDBC-464, DATAJDBC-318
	public void executeQueryWithParameterRequiringConversion() {

		Instant now = Instant.now();

		DummyEntity first = repository.save(createDummyEntity());
		first.setPointInTime(now.minusSeconds(1000L));
		first.setName("first");

		DummyEntity second = repository.save(createDummyEntity());
		second.setPointInTime(now.plusSeconds(1000L));
		second.setName("second");

		repository.saveAll(asList(first, second));

		assertThat(repository.after(now)) //
				.extracting(DummyEntity::getName) //
				.containsExactly("second");

		assertThat(repository.findAllByPointInTimeAfter(now)) //
				.extracting(DummyEntity::getName) //
				.containsExactly("second");
	}

	@Test // DATAJDBC-318
	public void queryMethodShouldEmitEvents() {

		repository.save(createDummyEntity());
		eventListener.events.clear();

		repository.findAllWithSql();

		assertThat(eventListener.events).hasSize(1).hasOnlyElementsOfType(AfterLoadEvent.class);
	}

	@Test // DATAJDBC-318
	public void queryMethodWithCustomRowMapperDoesNotEmitEvents() {

		repository.save(createDummyEntity());
		eventListener.events.clear();

		repository.findAllWithCustomMapper();

		assertThat(eventListener.events).isEmpty();
	}

	@Test // DATAJDBC-234
	public void findAllByQueryName() {

		repository.save(createDummyEntity());
		assertThat(repository.findAllByNamedQuery()).hasSize(1);
	}

	@Test // DATAJDBC-341
	public void findWithMissingQuery() {

		DummyEntity dummy = repository.save(createDummyEntity());

		DummyEntity loaded = repository.withMissingColumn(dummy.idProp);

		assertThat(loaded.idProp).isEqualTo(dummy.idProp);
		assertThat(loaded.name).isNull();
		assertThat(loaded.pointInTime).isNull();
	}

	@Test // DATAJDBC-529
	public void existsWorksAsExpected() {

		DummyEntity dummy = repository.save(createDummyEntity());

		assertSoftly(softly -> {

			softly.assertThat(repository.existsByName(dummy.getName())) //
					.describedAs("Positive") //
					.isTrue();
			softly.assertThat(repository.existsByName("not an existing name")) //
					.describedAs("Positive") //
					.isFalse();
		});
	}

	@Test // DATAJDBC-534
	public void countByQueryDerivation() {

		DummyEntity one = createDummyEntity();
		DummyEntity two = createDummyEntity();
		two.name = "other";
		DummyEntity three = createDummyEntity();

		repository.saveAll(asList(one, two, three));

		assertThat(repository.countByName(one.getName())).isEqualTo(2);
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

		List<DummyEntity> findAllByNamedQuery();

		List<DummyEntity> findAllByPointInTimeAfter(Instant instant);

		@Query("SELECT * FROM DUMMY_ENTITY")
		List<DummyEntity> findAllWithSql();

		@Query(value = "SELECT * FROM DUMMY_ENTITY", rowMapperClass = CustomRowMapper.class)
		List<DummyEntity> findAllWithCustomMapper();

		@Query("SELECT * FROM DUMMY_ENTITY WHERE POINT_IN_TIME > :threshhold")
		List<DummyEntity> after(@Param("threshhold") Instant threshhold);

		@Query("SELECT id_Prop from dummy_entity where id_Prop = :id")
		DummyEntity withMissingColumn(@Param("id") Long id);

		boolean existsByName(String name);

		int countByName(String name);
	}

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

		@Bean
		NamedQueries namedQueries() throws IOException {

			PropertiesFactoryBean properties = new PropertiesFactoryBean();
			properties.setLocation(new ClassPathResource("META-INF/jdbc-named-queries.properties"));
			properties.afterPropertiesSet();
			return new PropertiesBasedNamedQueries(properties.getObject());
		}

		@Bean
		MyEventListener eventListener() {
			return new MyEventListener();
		}
	}

	static class MyEventListener implements ApplicationListener<AbstractRelationalEvent<?>> {

		private List<AbstractRelationalEvent<?>> events = new ArrayList<>();

		@Override
		public void onApplicationEvent(AbstractRelationalEvent<?> event) {
			events.add(event);
		}
	}

	@Data
	static class DummyEntity {
		String name;
		Instant pointInTime;
		@Id private Long idProp;
	}

	static class CustomRowMapper implements RowMapper<DummyEntity> {

		@Override
		public DummyEntity mapRow(ResultSet rs, int rowNum) {
			return new DummyEntity();
		}
	}
}
