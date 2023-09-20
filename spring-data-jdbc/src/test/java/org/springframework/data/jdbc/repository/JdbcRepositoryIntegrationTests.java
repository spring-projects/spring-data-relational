/*
 * Copyright 2017-2023 the original author or authors.
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
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.ConditionalOnDatabase;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.event.AbstractRelationalEvent;
import org.springframework.data.relational.core.mapping.event.AfterConvertEvent;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.core.support.RepositoryFactoryCustomizer;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.data.support.WindowIterator;
import org.springframework.data.util.Streamable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.test.jdbc.JdbcTestUtils;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @author Christopher Klein
 * @author Mikhail Polivakha
 */
@IntegrationTest
public class JdbcRepositoryIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired MyEventListener eventListener;
	@Autowired RootRepository rootRepository;

	@Autowired WithDelimitedColumnRepository withDelimitedColumnRepository;

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

		assertThat(JdbcTestUtils.countRowsInTableWhere(template.getJdbcOperations(), "dummy_entity",
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
	public void insertsManyEntities() {

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

	@Test // GH-537
	void insertsOrUpdatesManyEntities() {

		DummyEntity entity = repository.save(createDummyEntity());
		entity.setName("something else");
		DummyEntity other = createDummyEntity();
		other.setName("others name");
		repository.saveAll(asList(other, entity));

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

		assertThat(eventListener.events).hasSize(1).hasOnlyElementsOfType(AfterConvertEvent.class);
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

	@Test
	void findAllByFirstnameWithLock() {

		DummyEntity dummyEntity = createDummyEntity();
		repository.save(dummyEntity);
		assertThat(repository.findAllByName(dummyEntity.getName())).hasSize(1);
	}

	@Test // GH-1022
	public void findAllByCustomQueryName() {

		repository.save(createDummyEntity());
		assertThat(repository.findAllByCustomNamedQuery()).hasSize(1);
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

	@Test // GH-619
	public void findBySpElWorksAsExpected() {
		DummyEntity r = repository.save(createDummyEntity());

		// assign the new id to the global ID provider holder; this is similar to Spring Security's SecurityContextHolder
		MyIdContextProvider.ExtensionRoot.ID = r.getIdProp();

		// expect, that we can find our newly created entity based upon the ID provider
		assertThat(repository.findWithSpEL().getIdProp()).isEqualTo(r.getIdProp());
	}

	@Test // GH-945
	@ConditionalOnDatabase(DatabaseType.POSTGRES)
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

	@Test // GH-935
	public void queryByOffsetDateTime() {

		Instant now = createDummyBeforeAndAfterNow();
		OffsetDateTime timeArgument = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(2));

		List<DummyEntity> entities = repository.findByOffsetDateTime(timeArgument);

		assertThat(entities).extracting(DummyEntity::getName).containsExactly("second");
	}

	@Test // GH-971
	public void stringQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		List<DummyProjection> result = repository.findProjectedWithSql(DummyProjection.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // GH-971
	public void stringQueryProjectionShouldReturnDtoProjectedEntities() {

		repository.save(createDummyEntity());

		List<DtoProjection> result = repository.findProjectedWithSql(DtoProjection.class);

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // GH-971
	public void partTreeQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		List<DummyProjection> result = repository.findProjectedByName("Entity Name");

		assertThat(result).hasSize(1);
		assertThat(result.get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // GH-971
	public void pageQueryProjectionShouldReturnProjectedEntities() {

		repository.save(createDummyEntity());

		Page<DummyProjection> result = repository.findPageProjectionByName("Entity Name", PageRequest.ofSize(10));

		assertThat(result).hasSize(1);
		assertThat(result.getContent().get(0).getName()).isEqualTo("Entity Name");
	}

	@Test // GH-974
	@ConditionalOnDatabase(DatabaseType.POSTGRES)
	void intervalCalculation() {

		repository.updateWithIntervalCalculation(23L, LocalDateTime.now());
	}

	@Test // GH-908
	void derivedQueryWithBooleanLiteralFindsCorrectValues() {

		repository.save(createDummyEntity());
		DummyEntity entity = createDummyEntity();
		entity.flag = true;
		entity = repository.save(entity);

		List<DummyEntity> result = repository.findByFlagTrue();

		assertThat(result).extracting(e -> e.idProp).containsExactly(entity.idProp);
	}

	@Test // GH-987
	void queryBySimpleReference() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = createDummyEntity();
		two.ref = AggregateReference.to(one.idProp);
		two = repository.save(two);

		List<DummyEntity> result = repository.findByRef(one.idProp.intValue());

		assertThat(result).extracting(e -> e.idProp).containsExactly(two.idProp);
	}

	@Test // GH-987
	void queryByAggregateReference() {

		DummyEntity one = repository.save(createDummyEntity());
		DummyEntity two = createDummyEntity();
		two.ref = AggregateReference.to(one.idProp);
		two = repository.save(two);

		List<DummyEntity> result = repository.findByRef(two.ref);

		assertThat(result).extracting(e -> e.idProp).containsExactly(two.idProp);
	}

	@Test // GH-1167
	void stringResult() {

		repository.save(createDummyEntity()); // just ensure we have data in the table

		assertThat(repository.returnInput("HELLO")).isEqualTo("HELLO");
	}

	@Test // GH-1167
	void nullStringResult() {

		repository.save(createDummyEntity()); // just ensure we have data in the table

		assertThat(repository.returnInput(null)).isNull();
	}

	@Test // GH-1212
	void queryByEnumTypeIn() {

		DummyEntity dummyA = new DummyEntity("dummyA");
		dummyA.setDirection(Direction.LEFT);
		DummyEntity dummyB = new DummyEntity("dummyB");
		dummyB.setDirection(Direction.CENTER);
		DummyEntity dummyC = new DummyEntity("dummyC");
		dummyC.setDirection(Direction.RIGHT);
		repository.saveAll(asList(dummyA, dummyB, dummyC));

		assertThat(repository.findByEnumTypeIn(Set.of(Direction.LEFT, Direction.RIGHT)))
				.extracting(DummyEntity::getDirection).containsExactlyInAnyOrder(Direction.LEFT, Direction.RIGHT);
	}

	@Test // GH-1212
	void queryByEnumTypeEqual() {

		DummyEntity dummyA = new DummyEntity("dummyA");
		dummyA.setDirection(Direction.LEFT);
		DummyEntity dummyB = new DummyEntity("dummyB");
		dummyB.setDirection(Direction.CENTER);
		DummyEntity dummyC = new DummyEntity("dummyC");
		dummyC.setDirection(Direction.RIGHT);
		repository.saveAll(asList(dummyA, dummyB, dummyC));

		assertThat(repository.findByEnumType(Direction.CENTER)).extracting(DummyEntity::getDirection)
				.containsExactlyInAnyOrder(Direction.CENTER);
	}

	@Test // GH-537
	void manyInsertsWithNestedEntities() {

		Root root1 = createRoot("root1");
		Root root2 = createRoot("root2");

		List<Root> savedRoots = rootRepository.saveAll(asList(root1, root2));

		List<Root> reloadedRoots = rootRepository.findAllByOrderByIdAsc();
		assertThat(reloadedRoots).isEqualTo(savedRoots);
		assertThat(reloadedRoots).hasSize(2);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(0), root1);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(1), root2);
	}

	@Test // GH-537
	@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES)
	void manyUpdatesWithNestedEntities() {

		Root root1 = createRoot("root1");
		Root root2 = createRoot("root2");
		List<Root> roots = rootRepository.saveAll(asList(root1, root2));
		Root savedRoot1 = roots.get(0);
		Root updatedRoot1 = new Root(savedRoot1.id, "updated" + savedRoot1.name,
				new Intermediate(savedRoot1.intermediate.id, "updated" + savedRoot1.intermediate.name,
						new Leaf(savedRoot1.intermediate.leaf.id, "updated" + savedRoot1.intermediate.leaf.name), emptyList()),
				savedRoot1.intermediates);
		Root savedRoot2 = roots.get(1);
		Root updatedRoot2 = new Root(savedRoot2.id, "updated" + savedRoot2.name, savedRoot2.intermediate,
				singletonList(
						new Intermediate(savedRoot2.intermediates.get(0).id, "updated" + savedRoot2.intermediates.get(0).name, null,
								singletonList(new Leaf(savedRoot2.intermediates.get(0).leaves.get(0).id,
										"updated" + savedRoot2.intermediates.get(0).leaves.get(0).name)))));

		List<Root> updatedRoots = rootRepository.saveAll(asList(updatedRoot1, updatedRoot2));

		List<Root> reloadedRoots = rootRepository.findAllByOrderByIdAsc();
		assertThat(reloadedRoots).isEqualTo(updatedRoots);
		assertThat(reloadedRoots).containsExactly(updatedRoot1, updatedRoot2);
	}

	@Test // GH-537
	@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES)
	void manyInsertsAndUpdatesWithNestedEntities() {

		Root root1 = createRoot("root1");
		Root savedRoot1 = rootRepository.save(root1);
		Root updatedRoot1 = new Root(savedRoot1.id, "updated" + savedRoot1.name,
				new Intermediate(savedRoot1.intermediate.id, "updated" + savedRoot1.intermediate.name,
						new Leaf(savedRoot1.intermediate.leaf.id, "updated" + savedRoot1.intermediate.leaf.name), emptyList()),
				savedRoot1.intermediates);
		Root root2 = createRoot("root2");
		List<Root> savedRoots = rootRepository.saveAll(asList(updatedRoot1, root2));

		List<Root> reloadedRoots = rootRepository.findAllByOrderByIdAsc();
		assertThat(reloadedRoots).isEqualTo(savedRoots);
		assertThat(reloadedRoots.get(0)).isEqualTo(updatedRoot1);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(1), root2);
	}

	@Test // GH-1192
	void findOneByExampleShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);
		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		dummyEntity2.setName("Diego");
		repository.save(dummyEntity2);

		Example<DummyEntity> diegoExample = Example.of(new DummyEntity("Diego"));
		Optional<DummyEntity> foundExampleDiego = repository.findOne(diegoExample);

		assertThat(foundExampleDiego.get().getName()).isEqualTo("Diego");
	}

	@Test // GH-1192
	void findOneByExampleMultipleMatchShouldGetOne() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		Example<DummyEntity> example = Example.of(createDummyEntity());

		assertThatThrownBy(() -> repository.findOne(example)).isInstanceOf(IncorrectResultSizeDataAccessException.class)
				.hasMessageContaining("expected 1, actual 2");
	}

	@Test // GH-1192
	void findOneByExampleShouldGetNone() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);
		repository.save(dummyEntity1);

		Example<DummyEntity> diegoExample = Example.of(new DummyEntity("NotExisting"));

		Optional<DummyEntity> foundExampleDiego = repository.findOne(diegoExample);

		assertThat(foundExampleDiego).isNotPresent();
	}

	@Test // GH-1192
	void findAllByExampleShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);
		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		dummyEntity2.setName("Diego");
		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(new DummyEntity("Diego"));

		Iterable<DummyEntity> allFound = repository.findAll(example);

		assertThat(allFound).extracting(DummyEntity::getName) //
				.containsExactly(example.getProbe().getName());
	}

	@Test // GH-1192
	void findAllByExampleMultipleMatchShouldGetOne() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		Example<DummyEntity> example = Example.of(createDummyEntity());

		Iterable<DummyEntity> allFound = repository.findAll(example);

		assertThat(allFound) //
				.hasSize(2) //
				.extracting(DummyEntity::getName) //
				.containsOnly(example.getProbe().getName());
	}

	@Test // GH-1192
	void findAllByExampleShouldGetNone() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		Example<DummyEntity> example = Example.of(new DummyEntity("NotExisting"));

		Iterable<DummyEntity> allFound = repository.findAll(example);

		assertThat(allFound).isEmpty();
	}

	@Test // GH-1192
	void findAllByExamplePageableShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		dummyEntity2.setName("Diego");

		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(new DummyEntity("Diego"));
		Pageable pageRequest = PageRequest.of(0, 10);

		Iterable<DummyEntity> allFound = repository.findAll(example, pageRequest);

		assertThat(allFound).extracting(DummyEntity::getName) //
				.containsExactly(example.getProbe().getName());
	}

	@Test // GH-1192
	void findAllByExamplePageableMultipleMatchShouldGetOne() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		Example<DummyEntity> example = Example.of(createDummyEntity());
		Pageable pageRequest = PageRequest.of(0, 10);

		Iterable<DummyEntity> allFound = repository.findAll(example, pageRequest);

		assertThat(allFound) //
				.hasSize(2) //
				.extracting(DummyEntity::getName) //
				.containsOnly(example.getProbe().getName());
	}

	@Test // GH-1192
	void findAllByExamplePageableShouldGetNone() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		Example<DummyEntity> example = Example.of(new DummyEntity("NotExisting"));
		Pageable pageRequest = PageRequest.of(0, 10);

		Iterable<DummyEntity> allFound = repository.findAll(example, pageRequest);

		assertThat(allFound).isEmpty();
	}

	@Test // GH-1192
	void findAllByExamplePageableOutsidePageShouldGetNone() {

		repository.save(createDummyEntity());
		repository.save(createDummyEntity());

		Example<DummyEntity> example = Example.of(createDummyEntity());
		Pageable pageRequest = PageRequest.of(10, 10);

		Iterable<DummyEntity> allFound = repository.findAll(example, pageRequest);

		assertThat(allFound) //
				.isNotNull() //
				.isEmpty();
	}

	@ParameterizedTest // GH-1192
	@MethodSource("findAllByExamplePageableSource")
	void findAllByExamplePageable(Pageable pageRequest, int size, int totalPages, List<String> notContains) {

		for (int i = 0; i < 100; i++) {
			DummyEntity dummyEntity = createDummyEntity();
			dummyEntity.setFlag(true);
			dummyEntity.setName("" + i);

			repository.save(dummyEntity);
		}

		DummyEntity dummyEntityExample = createDummyEntity();
		dummyEntityExample.setName(null);
		dummyEntityExample.setFlag(true);

		Example<DummyEntity> example = Example.of(dummyEntityExample);

		Page<DummyEntity> allFound = repository.findAll(example, pageRequest);

		// page has correct size
		assertThat(allFound) //
				.isNotNull() //
				.hasSize(size);

		// correct number of total
		assertThat(allFound.getTotalElements()).isEqualTo(100);

		assertThat(allFound.getTotalPages()).isEqualTo(totalPages);

		if (!notContains.isEmpty()) {
			assertThat(allFound) //
					.extracting(DummyEntity::getName) //
					.doesNotContain(notContains.toArray(new String[0]));
		}
	}

	public static Stream<Arguments> findAllByExamplePageableSource() {
		return Stream.of( //
				Arguments.of(PageRequest.of(0, 3), 3, 34, Arrays.asList("3", "4", "100")), //
				Arguments.of(PageRequest.of(1, 10), 10, 10, Arrays.asList("9", "20", "30")), //
				Arguments.of(PageRequest.of(2, 10), 10, 10, Arrays.asList("1", "2", "3")), //
				Arguments.of(PageRequest.of(33, 3), 1, 34, Collections.emptyList()), //
				Arguments.of(PageRequest.of(36, 3), 0, 34, Collections.emptyList()), //
				Arguments.of(PageRequest.of(0, 10000), 100, 1, Collections.emptyList()), //
				Arguments.of(PageRequest.of(100, 10000), 0, 1, Collections.emptyList()) //
		);
	}

	@Test // GH-1192
	void existsByExampleShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);
		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		dummyEntity2.setName("Diego");
		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(new DummyEntity("Diego"));

		boolean exists = repository.exists(example);

		assertThat(exists).isTrue();
	}

	@Test // GH-1192
	void existsByExampleMultipleMatchShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(createDummyEntity());

		boolean exists = repository.exists(example);
		assertThat(exists).isTrue();
	}

	@Test // GH-1192
	void existsByExampleShouldGetNone() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		Example<DummyEntity> example = Example.of(new DummyEntity("NotExisting"));

		boolean exists = repository.exists(example);

		assertThat(exists).isFalse();
	}

	@Test // GH-1192
	void existsByExampleComplex() {

		Instant pointInTime = Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(10000);

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();
		two.setName("Diego");
		two.setPointInTime(pointInTime);
		repository.save(two);

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName("Diego");
		exampleEntitiy.setPointInTime(pointInTime);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		boolean exists = repository.exists(example);
		assertThat(exists).isTrue();
	}

	@Test // GH-1192
	void countByExampleShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		dummyEntity2.setName("Diego");

		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(new DummyEntity("Diego"));

		long count = repository.count(example);

		assertThat(count).isOne();
	}

	@Test // GH-1192
	void countByExampleMultipleMatchShouldGetOne() {

		DummyEntity dummyEntity1 = createDummyEntity();
		repository.save(dummyEntity1);

		DummyEntity dummyEntity2 = createDummyEntity();
		repository.save(dummyEntity2);

		Example<DummyEntity> example = Example.of(createDummyEntity());

		long count = repository.count(example);
		assertThat(count).isEqualTo(2);
	}

	@Test // GH-1192
	void countByExampleShouldGetNone() {

		DummyEntity dummyEntity1 = createDummyEntity();
		dummyEntity1.setFlag(true);

		repository.save(dummyEntity1);

		Example<DummyEntity> example = Example.of(new DummyEntity("NotExisting"));

		long count = repository.count(example);

		assertThat(count).isNotNull().isZero();
	}

	@Test // GH-1192
	void countByExampleComplex() {

		Instant pointInTime = Instant.now().minusSeconds(10000).truncatedTo(ChronoUnit.MILLIS);
		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();
		two.setName("Diego");
		two.setPointInTime(pointInTime);
		repository.save(two);

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName("Diego");
		exampleEntitiy.setPointInTime(pointInTime);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		long count = repository.count(example);
		assertThat(count).isOne();
	}

	@Test // GH-1192
	void fetchByExampleFluentAllSimple() {

		String searchName = "Diego";
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

		DummyEntity two = createDummyEntity();

		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		two = repository.save(two);
		// certain databases consider it a great idea to assign default values to timestamp fields.
		// I'm looking at you MariaDb.
		two = repository.findById(two.idProp).orElseThrow();

		DummyEntity third = createDummyEntity();
		third.setName(searchName);
		third.setPointInTime(now.minusSeconds(200000));
		third = repository.save(third);
		// certain databases consider it a great idea to assign default values to timestamp fields.
		// I'm looking at you MariaDb.
		third = repository.findById(third.idProp).orElseThrow();

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		List<DummyEntity> matches = repository.findBy(example, p -> p.sortBy(Sort.by("pointInTime").descending()).all());
		assertThat(matches).containsExactly(two, third);
	}

	@Test // GH-1609
	void findByScrollPosition() {

		DummyEntity one = new DummyEntity("one");
		one.setFlag(true);

		DummyEntity two = new DummyEntity("two");
		two.setFlag(true);

		DummyEntity three = new DummyEntity("three");
		three.setFlag(true);

		DummyEntity four = new DummyEntity("four");
		four.setFlag(false);

		repository.saveAll(Arrays.asList(one, two, three, four));

		Example<DummyEntity> example = Example.of(one, ExampleMatcher.matching().withIgnorePaths("name", "idProp"));

		Window<DummyEntity> first = repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")))
				.scroll(ScrollPosition.offset());
		assertThat(first.map(DummyEntity::getName)).containsExactly("one", "three");

		Window<DummyEntity> second = repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")))
				.scroll(ScrollPosition.offset(2));
		assertThat(second.map(DummyEntity::getName)).containsExactly("two");

		WindowIterator<DummyEntity> iterator = WindowIterator.of(
				scrollPosition -> repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")).scroll(scrollPosition)))
				.startingAt(ScrollPosition.offset());

		List<String> result = Streamable.of(() -> iterator).stream().map(DummyEntity::getName).toList();

		assertThat(result).hasSize(3).containsExactly("one", "three", "two");
	}

	@Test // GH-1192
	void fetchByExampleFluentCountSimple() {

		String searchName = "Diego";
		Instant now = Instant.now();

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();

		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		repository.save(two);

		DummyEntity third = createDummyEntity();
		third.setName(searchName);
		third.setPointInTime(now.minusSeconds(200000));
		repository.save(third);

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		Long matches = repository.findBy(example, FluentQuery.FetchableFluentQuery::count);
		assertThat(matches).isEqualTo(2);
	}

	@Test // GH-1192
	void fetchByExampleFluentOnlyInstantFirstSimple() {

		String searchName = "Diego";
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();

		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		two = repository.save(two);
		// certain databases consider it a great idea to assign default values to timestamp fields.
		// I'm looking at you MariaDb.
		two = repository.findById(two.idProp).orElseThrow();

		DummyEntity third = createDummyEntity();
		third.setName(searchName);
		third.setPointInTime(now.minusSeconds(200000));
		repository.save(third);

		DummyEntity exampleEntity = createDummyEntity();
		exampleEntity.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntity);

		Optional<DummyEntity> matches = repository.findBy(example,
				p -> p.sortBy(Sort.by("pointInTime").descending()).first());

		assertThat(matches).contains(two);
	}

	@Test // GH-1192
	void fetchByExampleFluentOnlyInstantOneValueError() {

		String searchName = "Diego";
		Instant now = Instant.now();

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();
		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		repository.save(two);

		DummyEntity third = createDummyEntity();
		third.setName(searchName);
		third.setPointInTime(now.minusSeconds(200000));
		repository.save(third);

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		assertThatThrownBy(() -> repository.findBy(example, p -> p.sortBy(Sort.by("pointInTime").descending()).one()))
				.isInstanceOf(IncorrectResultSizeDataAccessException.class).hasMessageContaining("expected 1, actual 2");
	}

	@Test // GH-1192
	void fetchByExampleFluentOnlyInstantOneValueSimple() {

		String searchName = "Diego";
		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();
		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		two = repository.save(two);
		// certain databases consider it a great idea to assign default values to timestamp fields.
		// I'm looking at you MariaDb.
		two = repository.findById(two.idProp).orElseThrow();

		DummyEntity exampleEntitiy = createDummyEntity();
		exampleEntitiy.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntitiy);

		Optional<DummyEntity> match = repository.findBy(example, p -> p.sortBy(Sort.by("pointInTime").descending()).one());

		assertThat(match).contains(two);
	}

	@Test // GH-1192
	void fetchByExampleFluentOnlyInstantOneValueAsSimple() {

		String searchName = "Diego";
		Instant now = Instant.now();

		repository.save(createDummyEntity());

		DummyEntity two = createDummyEntity();
		two.setName(searchName);
		two.setPointInTime(now.minusSeconds(10000));
		two = repository.save(two);

		DummyEntity exampleEntity = createDummyEntity();
		exampleEntity.setName(searchName);

		Example<DummyEntity> example = Example.of(exampleEntity);

		Optional<DummyProjectExample> match = repository.findBy(example, p -> p.as(DummyProjectExample.class).one());

		assertThat(match.get().getName()).contains(two.getName());
	}

	@Test // GH-1405
	void withDelimitedColumnTest() {

		WithDelimitedColumn withDelimitedColumn = new WithDelimitedColumn();
		withDelimitedColumn.setType("TYPICAL");
		withDelimitedColumn.setIdentifier("UR-123");

		WithDelimitedColumn saved = withDelimitedColumnRepository.save(withDelimitedColumn);

		assertThat(saved.getId()).isNotNull();

		Optional<WithDelimitedColumn> inDatabase = withDelimitedColumnRepository.findById(saved.getId());

		assertThat(inDatabase).isPresent();
		assertThat(inDatabase.get().getIdentifier()).isEqualTo("UR-123");
	}

	private Root createRoot(String namePrefix) {

		return new Root(null, namePrefix,
				new Intermediate(null, namePrefix + "Intermediate", new Leaf(null, namePrefix + "Leaf"), emptyList()),
				singletonList(new Intermediate(null, namePrefix + "QualifiedIntermediate", null,
						singletonList(new Leaf(null, namePrefix + "QualifiedLeaf")))));
	}

	private void assertIsEqualToWithNonNullIds(Root reloadedRoot1, Root root1) {

		assertThat(reloadedRoot1.id).isNotNull();
		assertThat(reloadedRoot1.name).isEqualTo(root1.name);
		assertThat(reloadedRoot1.intermediate.id).isNotNull();
		assertThat(reloadedRoot1.intermediate.name).isEqualTo(root1.intermediate.name);
		assertThat(reloadedRoot1.intermediates.get(0).id).isNotNull();
		assertThat(reloadedRoot1.intermediates.get(0).name).isEqualTo(root1.intermediates.get(0).name);
		assertThat(reloadedRoot1.intermediate.leaf.id).isNotNull();
		assertThat(reloadedRoot1.intermediate.leaf.name).isEqualTo(root1.intermediate.leaf.name);
		assertThat(reloadedRoot1.intermediates.get(0).leaves.get(0).id).isNotNull();
		assertThat(reloadedRoot1.intermediates.get(0).leaves.get(0).name)
				.isEqualTo(root1.intermediates.get(0).leaves.get(0).name);
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

	interface DummyProjectExample {
		String getName();
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long>, QueryByExampleExecutor<DummyEntity> {

		@Lock(LockMode.PESSIMISTIC_WRITE)
		List<DummyEntity> findAllByName(String name);

		List<DummyEntity> findAllByNamedQuery();

		@Query(name = "DummyEntity.customQuery")
		List<DummyEntity> findAllByCustomNamedQuery();

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

		@Query("SELECT * FROM dummy_entity WHERE id_prop = :#{myext.id}")
		DummyEntity findWithSpEL();

		boolean existsByName(String name);

		int countByName(String name);

		@Query("select unnest( :ids )")
		List<Integer> unnestPrimitive(@Param("ids") int[] ids);

		Page<DummyEntity> findPageByNameContains(String name, Pageable pageable);

		Page<DummyProjection> findPageProjectionByName(String name, Pageable pageable);

		Slice<DummyEntity> findSliceByNameContains(String name, Pageable pageable);

		@Query("SELECT * FROM DUMMY_ENTITY WHERE OFFSET_DATE_TIME > :threshhold")
		List<DummyEntity> findByOffsetDateTime(@Param("threshhold") OffsetDateTime threshhold);

		@Modifying
		@Query("UPDATE dummy_entity SET point_in_time = :start - interval '30 minutes' WHERE id_prop = :id")
		void updateWithIntervalCalculation(@Param("id") Long id, @Param("start") LocalDateTime start);

		List<DummyEntity> findByFlagTrue();

		List<DummyEntity> findByRef(int ref);

		List<DummyEntity> findByRef(AggregateReference<DummyEntity, Long> ref);

		@Query("SELECT CAST(:hello AS CHAR(5)) FROM DUMMY_ENTITY")
		@Nullable
		String returnInput(@Nullable String hello);

		@Query("SELECT * FROM DUMMY_ENTITY WHERE DIRECTION IN (:directions)")
		List<DummyEntity> findByEnumTypeIn(Set<Direction> directions);

		@Query("SELECT * FROM DUMMY_ENTITY WHERE DIRECTION = :direction")
		List<DummyEntity> findByEnumType(Direction direction);
	}

	interface RootRepository extends ListCrudRepository<Root, Long> {
		List<Root> findAllByOrderByIdAsc();
	}

	interface WithDelimitedColumnRepository extends CrudRepository<WithDelimitedColumn, Long> {}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		RootRepository rootRepository() {
			return factory.getRepository(RootRepository.class);
		}

		@Bean
		WithDelimitedColumnRepository withDelimitedColumnRepository() {
			return factory.getRepository(WithDelimitedColumnRepository.class);
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

		@Bean
		public QueryMethodEvaluationContextProvider extensionAware(List<EvaluationContextExtension> exts) {
			return new ExtensionAwareQueryMethodEvaluationContextProvider(exts);
		}

		@Bean
		RepositoryFactoryCustomizer customizer(QueryMethodEvaluationContextProvider provider) {
			return repositoryFactory -> repositoryFactory.setEvaluationContextProvider(provider);
		}

		@Bean
		public EvaluationContextExtension evaluationContextExtension() {
			return new MyIdContextProvider();
		}

	}

	static final class Root {

		@Id private final Long id;
		private final String name;
		private final Intermediate intermediate;
		@MappedCollection(idColumn = "ROOT_ID", keyColumn = "ROOT_KEY") private final List<Intermediate> intermediates;

		public Root(Long id, String name, Intermediate intermediate, List<Intermediate> intermediates) {
			this.id = id;
			this.name = name;
			this.intermediate = intermediate;
			this.intermediates = intermediates;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Intermediate getIntermediate() {
			return this.intermediate;
		}

		public List<Intermediate> getIntermediates() {
			return this.intermediates;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Root other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$intermediate = this.getIntermediate();
			final Object other$intermediate = other.getIntermediate();
			if (!Objects.equals(this$intermediate, other$intermediate))
				return false;
			final Object this$intermediates = this.getIntermediates();
			final Object other$intermediates = other.getIntermediates();
			return Objects.equals(this$intermediates, other$intermediates);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $intermediate = this.getIntermediate();
			result = result * PRIME + ($intermediate == null ? 43 : $intermediate.hashCode());
			final Object $intermediates = this.getIntermediates();
			result = result * PRIME + ($intermediates == null ? 43 : $intermediates.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryIntegrationTests.Root(id=" + this.getId() + ", name=" + this.getName() + ", intermediate="
					+ this.getIntermediate() + ", intermediates=" + this.getIntermediates() + ")";
		}
	}

	@Table("WITH_DELIMITED_COLUMN")
	static class WithDelimitedColumn {
		@Id Long id;
		@Column("ORG.XTUNIT.IDENTIFIER") String identifier;
		@Column("STYPE") String type;

		public Long getId() {
			return this.id;
		}

		public String getIdentifier() {
			return this.identifier;
		}

		public String getType() {
			return this.type;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setIdentifier(String identifier) {
			this.identifier = identifier;
		}

		public void setType(String type) {
			this.type = type;
		}
	}

	static final class Intermediate {

		@Id private final Long id;
		private final String name;
		private final Leaf leaf;
		@MappedCollection(idColumn = "INTERMEDIATE_ID", keyColumn = "INTERMEDIATE_KEY") private final List<Leaf> leaves;

		public Intermediate(Long id, String name, Leaf leaf, List<Leaf> leaves) {
			this.id = id;
			this.name = name;
			this.leaf = leaf;
			this.leaves = leaves;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Leaf getLeaf() {
			return this.leaf;
		}

		public List<Leaf> getLeaves() {
			return this.leaves;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Intermediate other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$leaf = this.getLeaf();
			final Object other$leaf = other.getLeaf();
			if (!Objects.equals(this$leaf, other$leaf))
				return false;
			final Object this$leaves = this.getLeaves();
			final Object other$leaves = other.getLeaves();
			return Objects.equals(this$leaves, other$leaves);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $leaf = this.getLeaf();
			result = result * PRIME + ($leaf == null ? 43 : $leaf.hashCode());
			final Object $leaves = this.getLeaves();
			result = result * PRIME + ($leaves == null ? 43 : $leaves.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryIntegrationTests.Intermediate(id=" + this.getId() + ", name=" + this.getName() + ", leaf="
					+ this.getLeaf() + ", leaves=" + this.getLeaves() + ")";
		}
	}

	static final class Leaf {

		@Id private final Long id;
		private final String name;

		public Leaf(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Leaf other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			return Objects.equals(this$name, other$name);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryIntegrationTests.Leaf(id=" + this.getId() + ", name=" + this.getName() + ")";
		}
	}

	static class MyEventListener implements ApplicationListener<AbstractRelationalEvent<?>> {

		private final List<AbstractRelationalEvent<?>> events = new ArrayList<>();

		@Override
		public void onApplicationEvent(AbstractRelationalEvent<?> event) {
			events.add(event);
		}
	}

	// DATAJDBC-397
	public static class MyIdContextProvider implements EvaluationContextExtension {
		@Override
		public String getExtensionId() {
			return "myext";
		}

		public static class ExtensionRoot {
			// just public for testing purposes
			public static Long ID = 1L;

			public Long getId() {
				return ID;
			}
		}

		@Override
		public Object getRootObject() {
			return new ExtensionRoot();
		}
	}

	static class DummyEntity {

		String name;
		Instant pointInTime;
		OffsetDateTime offsetDateTime;
		@Id private Long idProp;
		boolean flag;
		AggregateReference<DummyEntity, Long> ref;
		Direction direction;

		public DummyEntity(String name) {
			this.name = name;
		}

		public DummyEntity() {}

		public String getName() {
			return this.name;
		}

		public Instant getPointInTime() {
			return this.pointInTime;
		}

		public OffsetDateTime getOffsetDateTime() {
			return this.offsetDateTime;
		}

		public Long getIdProp() {
			return this.idProp;
		}

		public boolean isFlag() {
			return this.flag;
		}

		public AggregateReference<DummyEntity, Long> getRef() {
			return this.ref;
		}

		public Direction getDirection() {
			return this.direction;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setPointInTime(Instant pointInTime) {
			this.pointInTime = pointInTime;
		}

		public void setOffsetDateTime(OffsetDateTime offsetDateTime) {
			this.offsetDateTime = offsetDateTime;
		}

		public void setIdProp(Long idProp) {
			this.idProp = idProp;
		}

		public void setFlag(boolean flag) {
			this.flag = flag;
		}

		public void setRef(AggregateReference<DummyEntity, Long> ref) {
			this.ref = ref;
		}

		public void setDirection(Direction direction) {
			this.direction = direction;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			DummyEntity that = (DummyEntity) o;
			return flag == that.flag && Objects.equals(name, that.name) && Objects.equals(pointInTime, that.pointInTime)
					&& Objects.equals(offsetDateTime, that.offsetDateTime) && Objects.equals(idProp, that.idProp)
					&& Objects.equals(ref, that.ref) && direction == that.direction;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, pointInTime, offsetDateTime, idProp, flag, ref, direction);
		}
	}

	enum Direction {
		LEFT, CENTER, RIGHT
	}

	interface DummyProjection {
		String getName();
	}

	static final class DtoProjection {
		private final String name;

		public DtoProjection(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final DtoProjection other))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			return Objects.equals(this$name, other$name);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryIntegrationTests.DtoProjection(name=" + this.getName() + ")";
		}
	}

	static class CustomRowMapper implements RowMapper<DummyEntity> {

		@Override
		public DummyEntity mapRow(ResultSet rs, int rowNum) {
			return new DummyEntity();
		}
	}
}
