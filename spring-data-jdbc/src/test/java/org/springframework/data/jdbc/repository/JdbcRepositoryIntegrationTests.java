/*
 * Copyright 2017-2021 the original author or authors.
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
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.io.IOException;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
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
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired MyEventListener eventListener;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");

		return entity;
	}

	@BeforeEach
	public void before() {

		repository.deleteAll();

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

	@Test // DATAJDBC-629
	public void deleteByIdList() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = repository.save(createDummyEntity());
		DummyEntity three = repository.save(createDummyEntity());

		repository.deleteAllById(asList(one.idProp, three.idProp));

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

		Instant now = createDummyBeforeAndAfterNow();

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

	@Test // DATAJDBC-604
	public void existsInWorksAsExpected() {

		DummyEntity dummy = repository.save(createDummyEntity());

		assertSoftly(softly -> {

			softly.assertThat(repository.existsByNameIn(dummy.getName())) //
					.describedAs("Positive") //
					.isTrue();
			softly.assertThat(repository.existsByNameIn()) //
					.describedAs("Negative") //
					.isFalse();
		});
	}

	@Test // DATAJDBC-604
	public void existsNotInWorksAsExpected() {

		DummyEntity dummy = repository.save(createDummyEntity());

		assertSoftly(softly -> {

			softly.assertThat(repository.existsByNameNotIn(dummy.getName())) //
					.describedAs("Positive") //
					.isFalse();
			softly.assertThat(repository.existsByNameNotIn()) //
					.describedAs("Negative") //
					.isTrue();
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

	@Test // #945
	@EnabledOnFeature(TestDatabaseFeatures.Feature.IS_POSTGRES)
	public void usePrimitiveArrayAsArgument() {
		assertThat(repository.unnestPrimitive(new int[] { 1, 2, 3 })).containsExactly(1, 2, 3);
	}

	@Test // GH-774
	public void pageByNameShouldReturnCorrectResult() {

		repository.saveAll(Arrays.asList(new DummyEntity("a1"), new DummyEntity("a2"), new DummyEntity("a3")));

		Page<DummyEntity> page = repository.findPageByNameContains("a", PageRequest.of(0, 5));

		assertThat(page.getContent()).hasSize(3);
		assertThat(page.getTotalElements()).isEqualTo(3);
		assertThat(page.getTotalPages()).isEqualTo(1);

		assertThat(repository.findPageByNameContains("a", PageRequest.of(0, 2)).getContent()).hasSize(2);
		assertThat(repository.findPageByNameContains("a", PageRequest.of(1, 2)).getContent()).hasSize(1);
	}

	@Test // GH-774
	public void sliceByNameShouldReturnCorrectResult() {

		repository.saveAll(Arrays.asList(new DummyEntity("a1"), new DummyEntity("a2"), new DummyEntity("a3")));

		Slice<DummyEntity> slice = repository.findSliceByNameContains("a", PageRequest.of(0, 5));

		assertThat(slice.getContent()).hasSize(3);
		assertThat(slice.hasNext()).isFalse();

		slice = repository.findSliceByNameContains("a", PageRequest.of(0, 2));

		assertThat(slice.getContent()).hasSize(2);
		assertThat(slice.hasNext()).isTrue();
	}

	@Test // #935
	public void queryByOffsetDateTime() {

		Instant now = createDummyBeforeAndAfterNow();
		OffsetDateTime timeArgument = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(2));

		List<DummyEntity> entities = repository.findByOffsetDateTime(timeArgument);

		assertThat(entities).extracting(DummyEntity::getName).containsExactly("second");
	}

	@Test // #971
	public void stringQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		List<DummyProjection> result = repository.findProjectedWithSql(DummyProjection.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // #971
	public void stringQueryProjectionShouldReturnDtoProjectedEntities() {

		repository.save(createDummyEntity());

		List<DtoProjection> result = repository.findProjectedWithSql(DtoProjection.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // #971
	public void partTreeQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		List<DummyProjection> result = repository.findProjectedByName("Entity Name");

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // #971
	public void pageQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		Page<DummyProjection> result = repository.findPageProjectionByName("Entity Name", PageRequest.ofSize(10));

		assertThat(result).hasSize(1);
		assertThat(result.getContent().get(0).getName()).isEqualTo("Entity Name");
	}

	private Instant createDummyBeforeAndAfterNow() {

		Instant now = Instant.now();

		DummyEntity first = createDummyEntity();
		Instant earlier = now.minusSeconds(1000L);
		OffsetDateTime earlierPlus3 = earlier.atOffset(ZoneOffset.ofHours(3));
		first.setPointInTime(earlier);
		first.offsetDateTime = earlierPlus3;

		first.setName("first");

		DummyEntity second = createDummyEntity();
		Instant later = now.plusSeconds(1000L);
		OffsetDateTime laterPlus3 = later.atOffset(ZoneOffset.ofHours(3));
		second.setPointInTime(later);
		second.offsetDateTime = laterPlus3;
		second.setName("second");

		repository.saveAll(asList(first, second));
		return now;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

		List<DummyEntity> findAllByNamedQuery();

		List<DummyEntity> findAllByPointInTimeAfter(Instant instant);

		@Query("SELECT * FROM DUMMY_ENTITY")
		List<DummyEntity> findAllWithSql();

		@Query("SELECT * FROM DUMMY_ENTITY")
		<T> List<T> findProjectedWithSql(Class<T> targetType);

		List<DummyProjection> findProjectedByName(String name);

		@Query(value = "SELECT * FROM DUMMY_ENTITY", rowMapperClass = CustomRowMapper.class)
		List<DummyEntity> findAllWithCustomMapper();

		@Query("SELECT * FROM DUMMY_ENTITY WHERE POINT_IN_TIME > :threshhold")
		List<DummyEntity> after(@Param("threshhold") Instant threshhold);

		@Query("SELECT id_Prop from dummy_entity where id_Prop = :id")
		DummyEntity withMissingColumn(@Param("id") Long id);

		boolean existsByNameIn(String... names);

		boolean existsByNameNotIn(String... names);

		boolean existsByName(String name);

		int countByName(String name);

		@Query("select unnest( :ids )")
		List<Integer> unnestPrimitive(@Param("ids") int[] ids);

		Page<DummyEntity> findPageByNameContains(String name, Pageable pageable);

		Page<DummyProjection> findPageProjectionByName(String name, Pageable pageable);

		Slice<DummyEntity> findSliceByNameContains(String name, Pageable pageable);

		@Query("SELECT * FROM DUMMY_ENTITY WHERE OFFSET_DATE_TIME > :threshhold")
		List<DummyEntity> findByOffsetDateTime(@Param("threshhold") OffsetDateTime threshhold);
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
	@NoArgsConstructor
	static class DummyEntity {
		String name;
		Instant pointInTime;
		OffsetDateTime offsetDateTime;
		@Id private Long idProp;

		public DummyEntity(String name) {
			this.name = name;
		}
	}

	interface DummyProjection {

		String getName();
	}

	@Value
	static class DtoProjection {

		String name;
	}

	static class CustomRowMapper implements RowMapper<DummyEntity> {

		@Override
		public DummyEntity mapRow(ResultSet rs, int rowNum) {
			return new DummyEntity();
		}
	}
}
