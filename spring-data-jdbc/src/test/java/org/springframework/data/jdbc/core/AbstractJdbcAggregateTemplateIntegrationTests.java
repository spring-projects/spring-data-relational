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
package org.springframework.data.jdbc.core;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.jdbc.testing.TestConfiguration.*;
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Persistable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.InsertOnlyProperty;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration tests for {@link JdbcAggregateTemplate}.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Tom Hombergs
 * @author Tyler Van Gorder
 * @author Clemens Hahn
 * @author Milan Milanov
 * @author Mikhail Polivakha
 * @author Chirag Tailor
 * @author Vincent Galloy
 */
@IntegrationTest
abstract class AbstractJdbcAggregateTemplateIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired NamedParameterJdbcOperations jdbcTemplate;
	@Autowired RelationalMappingContext mappingContext;
	@Autowired NamedParameterJdbcOperations jdbc;

	LegoSet legoSet = createLegoSet("Star Destroyer");

	/**
	 * creates an instance of {@link NoIdListChain4} with the following properties:
	 * <ul>
	 * <li>Each element has two children with indices 0 and 1.</li>
	 * <li>the xxxValue of each element is a {@literal v} followed by the indices used to navigate to the given instance.
	 * </li>
	 * </ul>
	 */
	private static NoIdListChain4 createNoIdTree() {

		NoIdListChain4 chain4 = new NoIdListChain4();
		chain4.fourValue = "v";

		IntStream.of(0, 1).forEach(i -> {

			NoIdListChain3 c3 = new NoIdListChain3();
			c3.threeValue = chain4.fourValue + i;
			chain4.chain3.add(c3);

			IntStream.of(0, 1).forEach(j -> {

				NoIdListChain2 c2 = new NoIdListChain2();
				c2.twoValue = c3.threeValue + j;
				c3.chain2.add(c2);

				IntStream.of(0, 1).forEach(k -> {

					NoIdListChain1 c1 = new NoIdListChain1();
					c1.oneValue = c2.twoValue + k;
					c2.chain1.add(c1);

					IntStream.of(0, 1).forEach(m -> {

						NoIdListChain0 c0 = new NoIdListChain0();
						c0.zeroValue = c1.oneValue + m;
						c1.chain0.add(c0);
					});
				});
			});
		});

		return chain4;
	}

	private static NoIdMapChain4 createNoIdMapTree() {

		NoIdMapChain4 chain4 = new NoIdMapChain4();
		chain4.fourValue = "v";

		IntStream.of(0, 1).forEach(i -> {

			NoIdMapChain3 c3 = new NoIdMapChain3();
			c3.threeValue = chain4.fourValue + i;
			chain4.chain3.put(asString(i), c3);

			IntStream.of(0, 1).forEach(j -> {

				NoIdMapChain2 c2 = new NoIdMapChain2();
				c2.twoValue = c3.threeValue + j;
				c3.chain2.put(asString(j), c2);

				IntStream.of(0, 1).forEach(k -> {

					NoIdMapChain1 c1 = new NoIdMapChain1();
					c1.oneValue = c2.twoValue + k;
					c2.chain1.put(asString(k), c1);

					IntStream.of(0, 1).forEach(it -> {

						NoIdMapChain0 c0 = new NoIdMapChain0();
						c0.zeroValue = c1.oneValue + it;
						c1.chain0.put(asString(it), c0);
					});
				});
			});
		});

		return chain4;
	}

	private static String asString(int i) {
		return "_" + i;
	}

	private static LegoSet createLegoSet(String name) {

		LegoSet entity = new LegoSet();
		entity.name = name;

		Manual manual = new Manual();
		manual.content = "Accelerates to 99% of light speed; Destroys almost everything. See https://what-if.xkcd.com/1/";
		entity.manual = manual;

		return entity;
	}

	@Test // GH-1446
	void findById() {

		WithInsertOnly entity = new WithInsertOnly();
		entity.insertOnly = "entity";
		entity = template.save(entity);

		WithInsertOnly other = new WithInsertOnly();
		other.insertOnly = "other";
		other = template.save(other);

		assertThat(template.findById(entity.id, WithInsertOnly.class).insertOnly).isEqualTo("entity");
		assertThat(template.findById(other.id, WithInsertOnly.class).insertOnly).isEqualTo("other");
	}

	@Test // GH-1446
	void findAllById() {

		WithInsertOnly entity = new WithInsertOnly();
		entity.insertOnly = "entity";
		entity = template.save(entity);

		WithInsertOnly other = new WithInsertOnly();
		other.insertOnly = "other";
		other = template.save(other);

		WithInsertOnly yetAnother = new WithInsertOnly();
		yetAnother.insertOnly = "yetAnother";
		yetAnother = template.save(yetAnother);

		Iterable<WithInsertOnly> reloadedById = template.findAllById(asList(entity.id, yetAnother.id),
				WithInsertOnly.class);
		assertThat(reloadedById).extracting(e -> e.id, e -> e.insertOnly)
				.containsExactlyInAnyOrder(tuple(entity.id, "entity"), tuple(yetAnother.id, "yetAnother"));
	}

	@Test // GH-1601
	void findAllByQuery() {

		template.save(SimpleListParent.of("one", "one_1"));
		SimpleListParent two = template.save(SimpleListParent.of("two", "two_1", "two_2"));
		template.save(SimpleListParent.of("three", "three_1", "three_2", "three_3"));

		CriteriaDefinition criteria = CriteriaDefinition.from(Criteria.where("id").is(two.id));
		Query query = Query.query(criteria);
		Iterable<SimpleListParent> reloadedById = template.findAll(query, SimpleListParent.class);

		assertThat(reloadedById).extracting(e -> e.id, e -> e.content.size()).containsExactly(tuple(two.id, 2));
	}

	@Test // GH-1601
	void findOneByQuery() {

		template.save(SimpleListParent.of("one", "one_1"));
		SimpleListParent two = template.save(SimpleListParent.of("two", "two_1", "two_2"));
		template.save(SimpleListParent.of("three", "three_1", "three_2", "three_3"));

		CriteriaDefinition criteria = CriteriaDefinition.from(Criteria.where("id").is(two.id));
		Query query = Query.query(criteria);
		Optional<SimpleListParent> reloadedById = template.findOne(query, SimpleListParent.class);

		assertThat(reloadedById).get().extracting(e -> e.id, e -> e.content.size()).containsExactly(two.id, 2);
	}

	@Test // GH-1601
	void findOneByQueryNothingFound() {

		template.save(SimpleListParent.of("one", "one_1"));
		SimpleListParent two = template.save(SimpleListParent.of("two", "two_1", "two_2"));
		template.save(SimpleListParent.of("three", "three_1", "three_2", "three_3"));

		CriteriaDefinition criteria = CriteriaDefinition.from(Criteria.where("id").is(4711));
		Query query = Query.query(criteria);
		Optional<SimpleListParent> reloadedById = template.findOne(query, SimpleListParent.class);

		assertThat(reloadedById).isEmpty();
	}

	@Test // GH-1601
	void findOneByQueryToManyResults() {

		template.save(SimpleListParent.of("one", "one_1"));
		SimpleListParent two = template.save(SimpleListParent.of("two", "two_1", "two_2"));
		template.save(SimpleListParent.of("three", "three_1", "three_2", "three_3"));

		CriteriaDefinition criteria = CriteriaDefinition.from(Criteria.where("id").not(two.id));
		Query query = Query.query(criteria);

		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> template.findOne(query, SimpleListParent.class));
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithReferencedEntityById() {

		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNotNull();

		assertThat(reloadedLegoSet.manual.id) //
				.isEqualTo(legoSet.manual.id) //
				.isNotNull();
		assertThat(reloadedLegoSet.manual.content).isEqualTo(legoSet.manual.content);
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadManyEntitiesWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class);

		assertThat(reloadedLegoSets) //
				.extracting("id", "manual.id", "manual.content") //
				.containsExactly(tuple(legoSet.id, legoSet.manual.id, legoSet.manual.content));
	}

	@Test // DATAJDBC-101
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadManyEntitiesWithReferencedEntitySorted() {

		template.save(createLegoSet("Lava"));
		template.save(createLegoSet("Star"));
		template.save(createLegoSet("Frozen"));

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class, Sort.by("name"));

		assertThat(reloadedLegoSets) //
				.extracting("name") //
				.containsExactly("Frozen", "Lava", "Star");
	}

	@Test // DATAJDBC-101
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadManyEntitiesWithReferencedEntitySortedAndPaged() {

		template.save(createLegoSet("Lava"));
		template.save(createLegoSet("Star"));
		template.save(createLegoSet("Frozen"));

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class, PageRequest.of(1, 2, Sort.by("name")));

		assertThat(reloadedLegoSets) //
				.extracting("name") //
				.containsExactly("Star");
	}

	@Test // GH-821
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS, SUPPORTS_NULL_PRECEDENCE })
	void saveAndLoadManyEntitiesWithReferencedEntitySortedWithNullPrecedence() {

		template.save(createLegoSet(null));
		template.save(createLegoSet("Star"));
		template.save(createLegoSet("Frozen"));

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class,
				Sort.by(new Sort.Order(Sort.Direction.ASC, "name", Sort.NullHandling.NULLS_LAST)));

		assertThat(reloadedLegoSets) //
				.extracting("name") //
				.containsExactly("Frozen", "Star", null);
	}

	@Test //
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS })
	void findByNonPropertySortFails() {

		assertThatThrownBy(() -> template.findAll(LegoSet.class, Sort.by("somethingNotExistant")))
				.isInstanceOf(InvalidPersistentPropertyPath.class);
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadManyEntitiesByIdWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAllById(singletonList(legoSet.id), LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(legoSet.id, legoSet.manual.id, legoSet.manual.content));
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithReferencedNullEntity() {

		legoSet.manual = null;

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNull();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndDeleteAnEntityWithReferencedEntity() {

		template.save(legoSet);

		template.delete(legoSet);

		assertThat(template.findAll(LegoSet.class)).isEmpty();
		assertThat(template.findAll(Manual.class)).isEmpty();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndDeleteAllWithReferencedEntity() {

		template.save(legoSet);

		template.deleteAll(LegoSet.class);

		assertThat(template.findAll(LegoSet.class)).isEmpty();
		assertThat(template.findAll(Manual.class)).isEmpty();
	}

	@Test // GH-537
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndDeleteAllByAggregateRootsWithReferencedEntity() {

		LegoSet legoSet1 = template.save(legoSet);
		LegoSet legoSet2 = template.save(createLegoSet("Some Name"));
		template.save(createLegoSet("Some other Name"));

		template.deleteAll(List.of(legoSet1, legoSet2));

		assertThat(template.findAll(LegoSet.class)).extracting(l -> l.name).containsExactly("Some other Name");
		assertThat(template.findAll(Manual.class)).hasSize(1);
	}

	@Test // GH-537
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndDeleteAllByIdsWithReferencedEntity() {

		LegoSet legoSet1 = template.save(legoSet);
		LegoSet legoSet2 = template.save(createLegoSet("Some Name"));
		template.save(createLegoSet("Some other Name"));

		template.deleteAllById(List.of(legoSet1.id, legoSet2.id), LegoSet.class);

		assertThat(template.findAll(LegoSet.class)).extracting(l -> l.name).containsExactly("Some other Name");
		assertThat(template.findAll(Manual.class)).hasSize(1);
	}

	@Test
	// GH-537
	void saveAndDeleteAllByAggregateRootsWithVersion() {

		AggregateWithImmutableVersion aggregate1 = new AggregateWithImmutableVersion(null, null);
		AggregateWithImmutableVersion aggregate2 = new AggregateWithImmutableVersion(null, null);
		AggregateWithImmutableVersion aggregate3 = new AggregateWithImmutableVersion(null, null);
		Iterator<AggregateWithImmutableVersion> savedAggregatesIterator = template
				.saveAll(List.of(aggregate1, aggregate2, aggregate3)).iterator();
		AggregateWithImmutableVersion savedAggregate1 = savedAggregatesIterator.next();
		AggregateWithImmutableVersion twiceSavedAggregate2 = template.save(savedAggregatesIterator.next());
		AggregateWithImmutableVersion twiceSavedAggregate3 = template.save(savedAggregatesIterator.next());

		assertThat(template.count(AggregateWithImmutableVersion.class)).isEqualTo(3);

		template.deleteAll(List.of(savedAggregate1, twiceSavedAggregate2, twiceSavedAggregate3));

		assertThat(template.count(AggregateWithImmutableVersion.class)).isEqualTo(0);
	}

	@Test
	// GH-1395
	void insertAndUpdateAllByAggregateRootsWithVersion() {

		AggregateWithImmutableVersion aggregate1 = new AggregateWithImmutableVersion(null, null);
		AggregateWithImmutableVersion aggregate2 = new AggregateWithImmutableVersion(null, null);
		AggregateWithImmutableVersion aggregate3 = new AggregateWithImmutableVersion(null, null);

		Iterator<AggregateWithImmutableVersion> savedAggregatesIterator = template
				.insertAll(List.of(aggregate1, aggregate2, aggregate3)).iterator();
		assertThat(template.count(AggregateWithImmutableVersion.class)).isEqualTo(3);

		AggregateWithImmutableVersion savedAggregate1 = savedAggregatesIterator.next();
		AggregateWithImmutableVersion twiceSavedAggregate2 = template.save(savedAggregatesIterator.next());
		AggregateWithImmutableVersion twiceSavedAggregate3 = template.save(savedAggregatesIterator.next());

		savedAggregatesIterator = template.updateAll(List.of(savedAggregate1, twiceSavedAggregate2, twiceSavedAggregate3))
				.iterator();

		assertThat(savedAggregatesIterator.next().version).isEqualTo(1);
		assertThat(savedAggregatesIterator.next().version).isEqualTo(2);
		assertThat(savedAggregatesIterator.next().version).isEqualTo(2);

		AggregateWithImmutableVersion.clearConstructorInvocationData();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS, SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES })
	void updateReferencedEntityFromNull() {

		legoSet.manual = (null);
		template.save(legoSet);

		Manual manual = new Manual();
		manual.id = 23L;
		manual.content = "Some content";
		legoSet.manual = manual;

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("Some content");
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void updateReferencedEntityToNull() {

		template.save(legoSet);

		legoSet.manual = null;

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNull();
		assertThat(template.findAll(Manual.class)).describedAs("Manuals failed to delete").isEmpty();
	}

	@Test
	// DATAJDBC-438
	void updateFailedRootDoesNotExist() {

		LegoSet entity = new LegoSet();
		entity.id = 100L; // does not exist in the database

		assertThatExceptionOfType(DbActionExecutionException.class) //
				.isThrownBy(() -> template.save(entity)) //
				.withCauseInstanceOf(IncorrectUpdateSemanticsDataAccessException.class);
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void replaceReferencedEntity() {

		template.save(legoSet);

		Manual manual = new Manual();
		manual.content = "other content";
		legoSet.manual = manual;

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("other content");
		assertThat(template.findAll(Manual.class)).describedAs("There should be only one manual").hasSize(1);
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS, TestDatabaseFeatures.Feature.SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES })
	void changeReferencedEntity() {

		template.save(legoSet);

		legoSet.manual.content = "new content";

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("new content");
	}

	@Test // DATAJDBC-266
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void oneToOneChildWithoutId() {

		OneToOneParent parent = new OneToOneParent();

		parent.content = "parent content";
		parent.child = new ChildNoId();
		parent.child.content = "child content";

		template.save(parent);

		OneToOneParent reloaded = template.findById(parent.id, OneToOneParent.class);

		assertThat(reloaded.child.content).isEqualTo("child content");
	}

	@Test // DATAJDBC-266
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void oneToOneNullChildWithoutId() {

		OneToOneParent parent = new OneToOneParent();

		parent.content = "parent content";
		parent.child = null;

		template.save(parent);

		OneToOneParent reloaded = template.findById(parent.id, OneToOneParent.class);

		assertThat(reloaded.child).isNull();
	}

	@Test // DATAJDBC-266
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void oneToOneNullAttributes() {

		OneToOneParent parent = new OneToOneParent();

		parent.content = "parent content";
		parent.child = new ChildNoId();

		template.save(parent);

		OneToOneParent reloaded = template.findById(parent.id, OneToOneParent.class);

		assertThat(reloaded.child).isNotNull();
	}

	@Test // DATAJDBC-125
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithSecondaryReferenceNull() {

		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.alternativeInstructions).isNull();
	}

	@Test // DATAJDBC-125
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithSecondaryReferenceNotNull() {

		legoSet.alternativeInstructions = new Manual();
		legoSet.alternativeInstructions.content = "alternative content";
		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.id, LegoSet.class);

		assertThat(reloadedLegoSet.alternativeInstructions).isNotNull();
		assertThat(reloadedLegoSet.alternativeInstructions.id).isNotNull();
		assertThat(reloadedLegoSet.alternativeInstructions.id).isNotEqualTo(reloadedLegoSet.manual.id);
		assertThat(reloadedLegoSet.alternativeInstructions.content)
				.isEqualTo(reloadedLegoSet.alternativeInstructions.content);
	}

	@Test // DATAJDBC-276
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithListOfElementsWithoutId() {

		ListParent entity = new ListParent();
		entity.name = "name";

		ElementNoId element = new ElementNoId();
		element.content = "content";

		entity.content.add(element);

		template.save(entity);

		ListParent reloaded = template.findById(entity.id, ListParent.class);

		assertThat(reloaded.content).extracting(e -> e.content).containsExactly("content");
	}

	@Test // GH-498 DATAJDBC-273
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadAnEntityWithListOfElementsInConstructor() {

		ElementNoId element = new ElementNoId();
		element.content = "content";
		ListParentAllArgs entity = new ListParentAllArgs("name", singletonList(element));

		entity = template.save(entity);

		ListParentAllArgs reloaded = template.findById(entity.id, ListParentAllArgs.class);

		assertThat(reloaded.content).extracting(e -> e.content).containsExactly("content");
	}

	@Test // DATAJDBC-259
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void saveAndLoadAnEntityWithArray() {

		ArrayOwner arrayOwner = new ArrayOwner();
		arrayOwner.digits = new String[] { "one", "two", "three" };

		ArrayOwner saved = template.save(arrayOwner);

		assertThat(saved.id).isNotNull();

		ArrayOwner reloaded = template.findById(saved.id, ArrayOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(new String[] { "one", "two", "three" });
	}

	@Test // DATAJDBC-259, DATAJDBC-512
	@EnabledOnFeature(SUPPORTS_MULTIDIMENSIONAL_ARRAYS)
	void saveAndLoadAnEntityWithMultidimensionalArray() {

		ArrayOwner arrayOwner = new ArrayOwner();
		arrayOwner.multidimensional = new String[][] { { "one-a", "two-a", "three-a" }, { "one-b", "two-b", "three-b" } };

		ArrayOwner saved = template.save(arrayOwner);

		assertThat(saved.id).isNotNull();

		ArrayOwner reloaded = template.findById(saved.id, ArrayOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.multidimensional)
				.isEqualTo(new String[][] { { "one-a", "two-a", "three-a" }, { "one-b", "two-b", "three-b" } });
	}

	@Test // DATAJDBC-259
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void saveAndLoadAnEntityWithList() {

		ListOwner arrayOwner = new ListOwner();
		arrayOwner.digits.addAll(asList("one", "two", "three"));

		ListOwner saved = template.save(arrayOwner);

		assertThat(saved.id).isNotNull();

		ListOwner reloaded = template.findById(saved.id, ListOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(asList("one", "two", "three"));
	}

	@Test // GH-1033
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void saveAndLoadAnEntityWithListOfDouble() {

		DoubleListOwner doubleListOwner = new DoubleListOwner();
		doubleListOwner.digits.addAll(asList(1.2, 1.3, 1.4));

		DoubleListOwner saved = template.save(doubleListOwner);

		assertThat(saved.id).isNotNull();

		DoubleListOwner reloaded = template.findById(saved.id, DoubleListOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(asList(1.2, 1.3, 1.4));
	}

	@Test // GH-1033, GH-1046
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void saveAndLoadAnEntityWithListOfFloat() {

		FloatListOwner floatListOwner = new FloatListOwner();
		final List<Float> values = asList(1.2f, 1.3f, 1.4f);
		floatListOwner.digits.addAll(values);

		FloatListOwner saved = template.save(floatListOwner);

		assertThat(saved.id).isNotNull();

		FloatListOwner reloaded = template.findById(saved.id, FloatListOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(values);
	}

	@Test // DATAJDBC-259
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void saveAndLoadAnEntityWithSet() {

		SetOwner setOwner = new SetOwner();
		setOwner.digits.addAll(asList("one", "two", "three"));

		SetOwner saved = template.save(setOwner);

		assertThat(saved.id).isNotNull();

		SetOwner reloaded = template.findById(saved.id, SetOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(new HashSet<>(asList("one", "two", "three")));
	}

	@Test
	// DATAJDBC-327
	void saveAndLoadAnEntityWithByteArray() {

		ByteArrayOwner owner = new ByteArrayOwner();
		owner.binaryData = new byte[] { 1, 23, 42 };

		ByteArrayOwner saved = template.save(owner);

		ByteArrayOwner reloaded = template.findById(saved.id, ByteArrayOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.binaryData).isEqualTo(new byte[] { 1, 23, 42 });
	}

	@Test // DATAJDBC-340
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadLongChain() {

		Chain4 chain4 = new Chain4();
		chain4.fourValue = "omega";
		chain4.chain3 = new Chain3();
		chain4.chain3.threeValue = "delta";
		chain4.chain3.chain2 = new Chain2();
		chain4.chain3.chain2.twoValue = "gamma";
		chain4.chain3.chain2.chain1 = new Chain1();
		chain4.chain3.chain2.chain1.oneValue = "beta";
		chain4.chain3.chain2.chain1.chain0 = new Chain0();
		chain4.chain3.chain2.chain1.chain0.zeroValue = "alpha";

		template.save(chain4);

		Chain4 reloaded = template.findById(chain4.four, Chain4.class);

		assertThat(reloaded).isNotNull();

		assertThat(reloaded.four).isEqualTo(chain4.four);
		assertThat(reloaded.chain3.chain2.chain1.chain0.zeroValue).isEqualTo(chain4.chain3.chain2.chain1.chain0.zeroValue);

		template.delete(chain4);

		assertThat(count("CHAIN0")).isEqualTo(0);
	}

	@Test // DATAJDBC-359
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void saveAndLoadLongChainWithoutIds() {

		NoIdChain4 chain4 = new NoIdChain4();
		chain4.fourValue = "omega";
		chain4.chain3 = new NoIdChain3();
		chain4.chain3.threeValue = "delta";
		chain4.chain3.chain2 = new NoIdChain2();
		chain4.chain3.chain2.twoValue = "gamma";
		chain4.chain3.chain2.chain1 = new NoIdChain1();
		chain4.chain3.chain2.chain1.oneValue = "beta";
		chain4.chain3.chain2.chain1.chain0 = new NoIdChain0();
		chain4.chain3.chain2.chain1.chain0.zeroValue = "alpha";

		template.save(chain4);

		assertThat(chain4.four).isNotNull();

		NoIdChain4 reloaded = template.findById(chain4.four, NoIdChain4.class);

		assertThat(reloaded).isNotNull();

		assertThat(reloaded.four).isEqualTo(chain4.four);
		assertThat(reloaded.chain3.chain2.chain1.chain0.zeroValue).isEqualTo(chain4.chain3.chain2.chain1.chain0.zeroValue);

		template.delete(chain4);

		assertThat(count("CHAIN0")).isEqualTo(0);
	}

	@Test
	// DATAJDBC-223
	void saveAndLoadLongChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());

		assertThat(saved.four).describedAs("Something went wrong during saving").isNotNull();

		NoIdListChain4 reloaded = template.findById(saved.four, NoIdListChain4.class);

		assertThat(reloaded.chain3).hasSameSizeAs(saved.chain3);
		assertThat(reloaded.chain3.get(0).chain2).hasSameSizeAs(saved.chain3.get(0).chain2);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test
	// DATAJDBC-223
	void shouldDeleteChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());
		template.deleteById(saved.four, NoIdListChain4.class);

		assertThat(count("NO_ID_LIST_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_LIST_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_LIST_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_LIST_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_LIST_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
	}

	@Test
	// DATAJDBC-223
	void saveAndLoadLongChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());

		assertThat(saved.four).isNotNull();

		NoIdMapChain4 reloaded = template.findById(saved.four, NoIdMapChain4.class);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test
	// DATAJDBC-223
	void shouldDeleteChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());
		template.deleteById(saved.four, NoIdMapChain4.class);

		assertThat(count("NO_ID_MAP_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_MAP_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_MAP_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_MAP_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
		assertThat(count("NO_ID_MAP_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
	}

	@Test // DATAJDBC-431
	@EnabledOnFeature(IS_HSQL)
	void readOnlyGetsLoadedButNotWritten() {

		WithReadOnly entity = new WithReadOnly();
		entity.name = "Alfred";
		entity.readOnly = "not used";

		template.save(entity);

		assertThat(
				jdbcTemplate.queryForObject("SELECT read_only FROM with_read_only", Collections.emptyMap(), String.class))
						.isEqualTo("from-db");
	}

	@Test
	// DATAJDBC-219 Test that immutable version attribute works as expected.
	void saveAndUpdateAggregateWithImmutableVersion() {

		AggregateWithImmutableVersion aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);
		assertThat(aggregate.version).isEqualTo(0L);

		Long id = aggregate.id;

		AggregateWithImmutableVersion reloadedAggregate = template.findById(id, aggregate.getClass());
		assertThat(reloadedAggregate.getVersion()).describedAs("version field should initially have the value 0")
				.isEqualTo(0L);

		AggregateWithImmutableVersion savedAgain = template.save(reloadedAggregate);
		AggregateWithImmutableVersion reloadedAgain = template.findById(id, aggregate.getClass());

		assertThat(savedAgain.version).describedAs("The object returned by save should have an increased version")
				.isEqualTo(1L);

		assertThat(reloadedAgain.getVersion()).describedAs("version field should increment by one with each save")
				.isEqualTo(1L);

		assertThatThrownBy(() -> template.save(new AggregateWithImmutableVersion(id, 0L)))
				.describedAs("saving an aggregate with an outdated version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);

		assertThatThrownBy(() -> template.save(new AggregateWithImmutableVersion(id, 2L)))
				.describedAs("saving an aggregate with a future version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test
	// GH-1137
	void testUpdateEntityWithVersionDoesNotTriggerAnewConstructorInvocation() {

		AggregateWithImmutableVersion aggregateWithImmutableVersion = new AggregateWithImmutableVersion(null, null);

		AggregateWithImmutableVersion savedRoot = template.save(aggregateWithImmutableVersion);

		assertThat(savedRoot).isNotNull();
		assertThat(savedRoot.version).isEqualTo(0L);

		assertThat(AggregateWithImmutableVersion.constructorInvocations).containsExactly(
				new ConstructorInvocation(null, null), // Initial invocation, done by client
				new ConstructorInvocation(null, savedRoot.version), // Assigning the version
				new ConstructorInvocation(savedRoot.id, savedRoot.version) // Assigning the id
		);

		AggregateWithImmutableVersion.clearConstructorInvocationData();

		AggregateWithImmutableVersion updatedRoot = template.save(savedRoot);

		assertThat(updatedRoot).isNotNull();
		assertThat(updatedRoot.version).isEqualTo(1L);

		// Expect only one assignment of the version to AggregateWithImmutableVersion
		assertThat(AggregateWithImmutableVersion.constructorInvocations)
				.containsOnly(new ConstructorInvocation(savedRoot.id, updatedRoot.version));
	}

	@Test
	// DATAJDBC-219 Test that a delete with a version attribute works as expected.
	void deleteAggregateWithVersion() {

		AggregateWithImmutableVersion aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);
		// as non-primitive versions start from 0, we need to save one more time to make version equal 1
		aggregate = template.save(aggregate);

		// Should have an ID and a version of 1.
		final Long id = aggregate.id;

		assertThatThrownBy(() -> template.delete(new AggregateWithImmutableVersion(id, 0L)))
				.describedAs("deleting an aggregate with an outdated version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);

		assertThatThrownBy(() -> template.delete(new AggregateWithImmutableVersion(id, 2L)))
				.describedAs("deleting an aggregate with a future version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);

		// This should succeed
		template.delete(aggregate);

		aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);

		// This should succeed, as version will not be used.
		template.deleteById(aggregate.id, AggregateWithImmutableVersion.class);

	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithLongVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithLongVersion(), Number::longValue);
	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithPrimitiveLongVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveLongVersion(), Number::longValue);
	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithIntegerVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithIntegerVersion(), Number::intValue);
	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithPrimitiveIntegerVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveIntegerVersion(), Number::intValue);
	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithShortVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithShortVersion(), Number::shortValue);
	}

	@Test
	// DATAJDBC-219
	void saveAndUpdateAggregateWithPrimitiveShortVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveShortVersion(), Number::shortValue);
	}

	@Test
	// GH-1254
	void saveAndUpdateAggregateWithIdAndNullVersion() {

		PersistableVersionedAggregate aggregate = new PersistableVersionedAggregate();
		aggregate.setVersion(null);
		aggregate.setId(23L);

		assertThatThrownBy(() -> template.save(aggregate)).isInstanceOf(DbActionExecutionException.class);
	}

	@Test // DATAJDBC-462
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	void resavingAnUnversionedEntity() {

		LegoSet legoSet = new LegoSet();

		LegoSet saved = template.save(legoSet);

		template.save(saved);
	}

	@Test // DATAJDBC-637
	@EnabledOnFeature(SUPPORTS_NANOSECOND_PRECISION)
	void saveAndLoadDateTimeWithFullPrecision() {

		WithLocalDateTime entity = new WithLocalDateTime();
		entity.id = 23L;
		entity.testTime = LocalDateTime.of(2005, 5, 5, 5, 5, 5, 123456789);

		template.insert(entity);

		WithLocalDateTime loaded = template.findById(23L, WithLocalDateTime.class);

		assertThat(loaded.testTime).isEqualTo(entity.testTime);
	}

	@Test
	// DATAJDBC-637
	void saveAndLoadDateTimeWithMicrosecondPrecision() {

		WithLocalDateTime entity = new WithLocalDateTime();
		entity.id = 23L;
		entity.testTime = LocalDateTime.of(2005, 5, 5, 5, 5, 5, 123456000);

		template.insert(entity);

		WithLocalDateTime loaded = template.findById(23L, WithLocalDateTime.class);

		assertThat(loaded.testTime).isEqualTo(entity.testTime);
	}

	@Test
	// GH-777
	void insertWithIdOnly() {

		WithIdOnly entity = new WithIdOnly();

		assertThat(template.save(entity).id).isNotNull();
	}

	@Test
	// GH-1309
	void updateIdOnlyAggregate() {

		WithIdOnly entity = new WithIdOnly();

		assertThat(template.save(entity).id).isNotNull();

		template.save(entity);
	}

	@Test
	// GH-637
	void insertOnlyPropertyDoesNotGetUpdated() {

		WithInsertOnly entity = new WithInsertOnly();
		entity.insertOnly = "first value";

		assertThat(template.save(entity).id).isNotNull();

		entity.insertOnly = "second value";
		template.save(entity);

		assertThat(template.findById(entity.id, WithInsertOnly.class).insertOnly).isEqualTo("first value");
	}

	@Test // GH-1460
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	void readEnumArray() {

		EnumArrayOwner entity = new EnumArrayOwner();
		entity.digits = new Color[] { Color.BLUE };

		template.save(entity);

		assertThat(template.findById(entity.id, EnumArrayOwner.class).digits).isEqualTo(new Color[] { Color.BLUE });
	}

	@Test // GH-1448
	void multipleCollections() {

		MultipleCollections aggregate = new MultipleCollections();
		aggregate.name = "aggregate";

		aggregate.listElements.add(new ListElement("one"));
		aggregate.listElements.add(new ListElement("two"));
		aggregate.listElements.add(new ListElement("three"));

		aggregate.setElements.add(new SetElement("one"));
		aggregate.setElements.add(new SetElement("two"));

		aggregate.mapElements.put("alpha", new MapElement("one"));
		aggregate.mapElements.put("beta", new MapElement("two"));
		aggregate.mapElements.put("gamma", new MapElement("three"));
		aggregate.mapElements.put("delta", new MapElement("four"));

		template.save(aggregate);

		MultipleCollections reloaded = template.findById(aggregate.id, MultipleCollections.class);

		assertThat(reloaded.name).isEqualTo(aggregate.name);

		assertThat(reloaded.listElements).containsExactly(aggregate.listElements.get(0), aggregate.listElements.get(1),
				aggregate.listElements.get(2));

		assertThat(reloaded.setElements).containsExactlyInAnyOrder(aggregate.setElements.toArray(new SetElement[0]));

		assertThat(reloaded.mapElements.get("alpha")).isEqualTo(new MapElement("one"));
		assertThat(reloaded.mapElements.get("beta")).isEqualTo(new MapElement("two"));
		assertThat(reloaded.mapElements.get("gamma")).isEqualTo(new MapElement("three"));
		assertThat(reloaded.mapElements.get("delta")).isEqualTo(new MapElement("four"));
	}

	@Test // GH-1448
	void multipleCollectionsWithEmptySet() {

		MultipleCollections aggregate = new MultipleCollections();
		aggregate.name = "aggregate";

		aggregate.listElements.add(new ListElement("one"));
		aggregate.listElements.add(new ListElement("two"));
		aggregate.listElements.add(new ListElement("three"));

		aggregate.mapElements.put("alpha", new MapElement("one"));
		aggregate.mapElements.put("beta", new MapElement("two"));
		aggregate.mapElements.put("gamma", new MapElement("three"));
		aggregate.mapElements.put("delta", new MapElement("four"));

		template.save(aggregate);

		MultipleCollections reloaded = template.findById(aggregate.id, MultipleCollections.class);

		assertThat(reloaded.name).isEqualTo(aggregate.name);

		assertThat(reloaded.listElements).containsExactly(aggregate.listElements.get(0), aggregate.listElements.get(1),
				aggregate.listElements.get(2));

		assertThat(reloaded.setElements).containsExactlyInAnyOrder(aggregate.setElements.toArray(new SetElement[0]));

		assertThat(reloaded.mapElements.get("alpha")).isEqualTo(new MapElement("one"));
		assertThat(reloaded.mapElements.get("beta")).isEqualTo(new MapElement("two"));
		assertThat(reloaded.mapElements.get("gamma")).isEqualTo(new MapElement("three"));
		assertThat(reloaded.mapElements.get("delta")).isEqualTo(new MapElement("four"));
	}

	@Test // GH-1448
	void multipleCollectionsWithEmptyList() {

		MultipleCollections aggregate = new MultipleCollections();
		aggregate.name = "aggregate";

		aggregate.setElements.add(new SetElement("one"));
		aggregate.setElements.add(new SetElement("two"));

		aggregate.mapElements.put("alpha", new MapElement("one"));
		aggregate.mapElements.put("beta", new MapElement("two"));
		aggregate.mapElements.put("gamma", new MapElement("three"));
		aggregate.mapElements.put("delta", new MapElement("four"));

		template.save(aggregate);

		MultipleCollections reloaded = template.findById(aggregate.id, MultipleCollections.class);

		assertThat(reloaded.name).isEqualTo(aggregate.name);

		assertThat(reloaded.listElements).containsExactly();

		assertThat(reloaded.setElements).containsExactlyInAnyOrder(aggregate.setElements.toArray(new SetElement[0]));

		assertThat(reloaded.mapElements.get("alpha")).isEqualTo(new MapElement("one"));
		assertThat(reloaded.mapElements.get("beta")).isEqualTo(new MapElement("two"));
		assertThat(reloaded.mapElements.get("gamma")).isEqualTo(new MapElement("three"));
		assertThat(reloaded.mapElements.get("delta")).isEqualTo(new MapElement("four"));
	}

	private <T extends Number> void saveAndUpdateAggregateWithVersion(VersionedAggregate aggregate,
			Function<Number, T> toConcreteNumber) {
		saveAndUpdateAggregateWithVersion(aggregate, toConcreteNumber, 0);
	}

	private <T extends Number> void saveAndUpdateAggregateWithPrimitiveVersion(VersionedAggregate aggregate,
			Function<Number, T> toConcreteNumber) {
		saveAndUpdateAggregateWithVersion(aggregate, toConcreteNumber, 1);
	}

	private <T extends Number> void saveAndUpdateAggregateWithVersion(VersionedAggregate aggregate,
			Function<Number, T> toConcreteNumber, int initialId) {

		template.save(aggregate);

		VersionedAggregate reloadedAggregate = template.findById(aggregate.id, aggregate.getClass());
		assertThat(reloadedAggregate.getVersion()) //
				.withFailMessage("version field should initially have the value 0")
				.isEqualTo(toConcreteNumber.apply(initialId));
		template.save(reloadedAggregate);

		VersionedAggregate updatedAggregate = template.findById(aggregate.id, aggregate.getClass());
		assertThat(updatedAggregate.getVersion()) //
				.withFailMessage("version field should increment by one with each save")
				.isEqualTo(toConcreteNumber.apply(initialId + 1));

		reloadedAggregate.setVersion(toConcreteNumber.apply(initialId));
		assertThatThrownBy(() -> template.save(reloadedAggregate))
				.withFailMessage("saving an aggregate with an outdated version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);

		reloadedAggregate.setVersion(toConcreteNumber.apply(initialId + 2));
		assertThatThrownBy(() -> template.save(reloadedAggregate))
				.withFailMessage("saving an aggregate with a future version should raise an exception")
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	private Long count(String tableName) {
		return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, emptyMap(), Long.class);
	}

	enum Color {
		BLUE
	}

	@Table("ARRAY_OWNER")
	private static class EnumArrayOwner {
		@Id Long id;

		Color[] digits;
	}

	@Table("ARRAY_OWNER")
	private static class ArrayOwner {
		@Id Long id;

		String[] digits;
		String[][] multidimensional;
	}

	private static class ByteArrayOwner {
		@Id Long id;

		byte[] binaryData;
	}

	@Table("ARRAY_OWNER")
	private static class ListOwner {
		@Id Long id;

		List<String> digits = new ArrayList<>();
	}

	@Table("ARRAY_OWNER")
	private static class SetOwner {
		@Id Long id;

		Set<String> digits = new HashSet<>();
	}

	private static class DoubleListOwner {

		@Id Long id;

		List<Double> digits = new ArrayList<>();
	}

	private static class FloatListOwner {

		@Id Long id;

		List<Float> digits = new ArrayList<>();
	}

	static class LegoSet {

		@Column("id1")
		@Id private Long id;

		private String name;

		private Manual manual;
		@Column("alternative") private Manual alternativeInstructions;
	}

	static class Manual {

		@Column("id2")
		@Id private Long id;
		private String content;

	}

	@SuppressWarnings("unused")
	static class OneToOneParent {

		@Column("id3")
		@Id private Long id;
		private String content;

		private ChildNoId child;
	}

	static class ChildNoId {
		private String content;
	}

	@SuppressWarnings("unused")
	static class SimpleListParent {

		@Id private Long id;
		String name;
		List<ElementNoId> content = new ArrayList<>();

		static SimpleListParent of(String name, String... contents) {

			SimpleListParent parent = new SimpleListParent();
			parent.name = name;

			for (String content : contents) {

				ElementNoId element = new ElementNoId();
				element.content = content;
				parent.content.add(element);
			}

			return parent;
		}
	}

	@Table("LIST_PARENT")
	@SuppressWarnings("unused")
	static class ListParent {

		@Column("id4")
		@Id private Long id;
		String name;
		@MappedCollection(idColumn = "LIST_PARENT") List<ElementNoId> content = new ArrayList<>();
	}

	@Table("LIST_PARENT")
	static class ListParentAllArgs {

		@Column("id4")
		@Id private final Long id;
		private final String name;
		@MappedCollection(idColumn = "LIST_PARENT") private final List<ElementNoId> content = new ArrayList<>();

		@PersistenceCreator
		ListParentAllArgs(Long id, String name, List<ElementNoId> content) {

			this.id = id;
			this.name = name;
			this.content.addAll(content);
		}

		ListParentAllArgs(String name, List<ElementNoId> content) {
			this(null, name, content);
		}
	}

	static class ElementNoId {
		private String content;
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	@SuppressWarnings("unused")
	static class Chain0 {
		@Id Long zero;
		String zeroValue;
	}

	@SuppressWarnings("unused")
	static class Chain1 {
		@Id Long one;
		String oneValue;
		Chain0 chain0;
	}

	@SuppressWarnings("unused")
	static class Chain2 {
		@Id Long two;
		String twoValue;
		Chain1 chain1;
	}

	@SuppressWarnings("unused")
	static class Chain3 {
		@Id Long three;
		String threeValue;
		Chain2 chain2;
	}

	static class Chain4 {
		@Id Long four;
		String fourValue;
		Chain3 chain3;
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	static class NoIdChain0 {
		String zeroValue;
	}

	static class NoIdChain1 {
		String oneValue;
		NoIdChain0 chain0;
	}

	static class NoIdChain2 {
		String twoValue;
		NoIdChain1 chain1;
	}

	static class NoIdChain3 {
		String threeValue;
		NoIdChain2 chain2;
	}

	static class NoIdChain4 {
		@Id Long four;
		String fourValue;
		NoIdChain3 chain3;
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	static class NoIdListChain0 {
		String zeroValue;

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdListChain0 that = (NoIdListChain0) o;
			return Objects.equals(zeroValue, that.zeroValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(zeroValue);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [zeroValue='" + zeroValue + '\'' + ']';
			return sb;
		}
	}

	static class NoIdListChain1 {
		String oneValue;
		List<NoIdListChain0> chain0 = new ArrayList<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdListChain1 that = (NoIdListChain1) o;
			return Objects.equals(oneValue, that.oneValue) && Objects.equals(chain0, that.chain0);
		}

		@Override
		public int hashCode() {
			return Objects.hash(oneValue, chain0);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [oneValue='" + oneValue + '\'' + ", chain0=" + chain0 + ']';
			return sb;
		}
	}

	static class NoIdListChain2 {
		String twoValue;
		List<NoIdListChain1> chain1 = new ArrayList<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdListChain2 that = (NoIdListChain2) o;
			return Objects.equals(twoValue, that.twoValue) && Objects.equals(chain1, that.chain1);
		}

		@Override
		public int hashCode() {
			return Objects.hash(twoValue, chain1);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [twoValue='" + twoValue + '\'' + ", chain1=" + chain1 + ']';
			return sb;
		}
	}

	static class NoIdListChain3 {
		String threeValue;
		List<NoIdListChain2> chain2 = new ArrayList<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdListChain3 that = (NoIdListChain3) o;
			return Objects.equals(threeValue, that.threeValue) && Objects.equals(chain2, that.chain2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(threeValue, chain2);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [threeValue='" + threeValue + '\'' + ", chain2=" + chain2 + ']';
			return sb;
		}
	}

	static class NoIdListChain4 {
		@Id Long four;
		String fourValue;
		List<NoIdListChain3> chain3 = new ArrayList<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdListChain4 that = (NoIdListChain4) o;
			return Objects.equals(four, that.four) && Objects.equals(fourValue, that.fourValue)
					&& Objects.equals(chain3, that.chain3);
		}

		@Override
		public int hashCode() {
			return Objects.hash(four, fourValue, chain3);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [four=" + four + ", fourValue='" + fourValue + '\'' + ", chain3="
					+ chain3 + ']';
			return sb;
		}

	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	static class NoIdMapChain0 {
		String zeroValue;

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdMapChain0 that = (NoIdMapChain0) o;
			return Objects.equals(zeroValue, that.zeroValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(zeroValue);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [zeroValue='" + zeroValue + '\'' + ']';
			return sb;
		}
	}

	static class NoIdMapChain1 {
		String oneValue;
		Map<String, NoIdMapChain0> chain0 = new HashMap<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdMapChain1 that = (NoIdMapChain1) o;
			return Objects.equals(oneValue, that.oneValue) && Objects.equals(chain0, that.chain0);
		}

		@Override
		public int hashCode() {
			return Objects.hash(oneValue, chain0);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [oneValue='" + oneValue + '\'' + ", chain0=" + chain0 + ']';
			return sb;
		}
	}

	static class NoIdMapChain2 {
		String twoValue;
		Map<String, NoIdMapChain1> chain1 = new HashMap<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdMapChain2 that = (NoIdMapChain2) o;
			return Objects.equals(twoValue, that.twoValue) && Objects.equals(chain1, that.chain1);
		}

		@Override
		public int hashCode() {
			return Objects.hash(twoValue, chain1);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [twoValue='" + twoValue + '\'' + ", chain1=" + chain1 + ']';
			return sb;
		}
	}

	static class NoIdMapChain3 {
		String threeValue;
		Map<String, NoIdMapChain2> chain2 = new HashMap<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdMapChain3 that = (NoIdMapChain3) o;
			return Objects.equals(threeValue, that.threeValue) && Objects.equals(chain2, that.chain2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(threeValue, chain2);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [threeValue='" + threeValue + '\'' + ", chain2=" + chain2 + ']';
			return sb;
		}
	}

	static class NoIdMapChain4 {
		@Id Long four;
		String fourValue;
		Map<String, NoIdMapChain3> chain3 = new HashMap<>();

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NoIdMapChain4 that = (NoIdMapChain4) o;
			return Objects.equals(four, that.four) && Objects.equals(fourValue, that.fourValue)
					&& Objects.equals(chain3, that.chain3);
		}

		@Override
		public int hashCode() {
			return Objects.hash(four, fourValue, chain3);
		}

		@Override
		public String toString() {
			String sb = getClass().getSimpleName() + " [four=" + four + ", fourValue='" + fourValue + '\'' + ", chain3="
					+ chain3 + ']';
			return sb;
		}
	}

	@SuppressWarnings("unused")
	static class WithReadOnly {
		@Id Long id;
		String name;
		@ReadOnlyProperty String readOnly;
	}

	static abstract class VersionedAggregate {

		@Id private Long id;

		abstract Number getVersion();

		abstract void setVersion(Number newVersion);
	}

	@Table("VERSIONED_AGGREGATE")
	static class PersistableVersionedAggregate implements Persistable<Long> {

		@Id private Long id;

		@Version Long version;

		@Override
		public boolean isNew() {
			return getId() == null;
		}

		@Override
		public Long getId() {
			return this.id;
		}

		public Long getVersion() {
			return this.version;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setVersion(Long version) {
			this.version = version;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static final class AggregateWithImmutableVersion {

		@Id private final Long id;
		@Version private final Long version;

		private final static List<ConstructorInvocation> constructorInvocations = new ArrayList<>();

		public static void clearConstructorInvocationData() {
			constructorInvocations.clear();
		}

		public AggregateWithImmutableVersion(Long id, Long version) {

			constructorInvocations.add(new ConstructorInvocation(id, version));
			this.id = id;
			this.version = version;
		}

		public Long getId() {
			return this.id;
		}

		public Long getVersion() {
			return this.version;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final AggregateWithImmutableVersion other))
				return false;
			final Object this$id = this.id;
			final Object other$id = other.id;
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$version = this.getVersion();
			final Object other$version = other.getVersion();
			return Objects.equals(this$version, other$version);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.id;
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $version = this.getVersion();
			result = result * PRIME + ($version == null ? 43 : $version.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcAggregateTemplateIntegrationTests.AggregateWithImmutableVersion(id=" + this.id + ", version="
					+ this.getVersion() + ")";
		}

		public AggregateWithImmutableVersion withId(Long id) {
			return this.id == id ? this : new AggregateWithImmutableVersion(id, this.version);
		}

		public AggregateWithImmutableVersion withVersion(Long version) {
			return this.version == version ? this : new AggregateWithImmutableVersion(this.id, version);
		}
	}

	private static final class ConstructorInvocation {

		private final Long id;
		private final Long version;

		public ConstructorInvocation(Long id, Long version) {
			this.id = id;
			this.version = version;
		}

		public Long getId() {
			return this.id;
		}

		public Long getVersion() {
			return this.version;
		}

		public String toString() {
			return "JdbcAggregateTemplateIntegrationTests.ConstructorInvocation(id=" + this.id + ", version="
					+ this.getVersion() + ")";
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final ConstructorInvocation other))
				return false;
			final Object this$id = this.id;
			final Object other$id = other.id;
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$version = this.getVersion();
			final Object other$version = other.getVersion();
			return Objects.equals(this$version, other$version);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.id;
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $version = this.getVersion();
			result = result * PRIME + ($version == null ? 43 : $version.hashCode());
			return result;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithLongVersion extends VersionedAggregate {

		@Version private Long version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Long) newVersion;
		}

		@Override
		public Long getVersion() {
			return this.version;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithPrimitiveLongVersion extends VersionedAggregate {

		@Version private long version;

		@Override
		Number getVersion() {
			return this.version;
		}

		@Override
		void setVersion(Number newVersion) {
			this.version = (long) newVersion;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithIntegerVersion extends VersionedAggregate {

		@Version private Integer version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Integer) newVersion;
		}

		@Override
		public Integer getVersion() {
			return this.version;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithPrimitiveIntegerVersion extends VersionedAggregate {

		@Version private int version;

		@Override
		Number getVersion() {
			return this.version;
		}

		@Override
		void setVersion(Number newVersion) {
			this.version = (int) newVersion;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithShortVersion extends VersionedAggregate {

		@Version private Short version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Short) newVersion;
		}

		@Override
		public Short getVersion() {
			return this.version;
		}
	}

	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithPrimitiveShortVersion extends VersionedAggregate {

		@Version private short version;

		@Override
		Number getVersion() {
			return this.version;
		}

		@Override
		void setVersion(Number newVersion) {
			this.version = (short) newVersion;
		}
	}

	@Table
	static class WithLocalDateTime {

		@Id Long id;
		LocalDateTime testTime;
	}

	@Table
	static class WithIdOnly {
		@Id Long id;
	}

	@Table
	static class WithInsertOnly {
		@Id Long id;
		@InsertOnlyProperty String insertOnly;
	}

	@Table
	static class MultipleCollections {
		@Id Long id;
		String name;
		List<ListElement> listElements = new ArrayList<>();
		Set<SetElement> setElements = new HashSet<>();
		Map<String, MapElement> mapElements = new HashMap<>();
	}

	record ListElement(String name) {
	}

	record SetElement(String name) {
	}

	record MapElement(String name) {
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		TestClass testClass() {
			return TestClass.of(JdbcAggregateTemplateIntegrationTests.class);
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}

	@ContextConfiguration(classes = Config.class)
	static class JdbcAggregateTemplateIntegrationTests extends AbstractJdbcAggregateTemplateIntegrationTests {}

	@ActiveProfiles(value = PROFILE_SINGLE_QUERY_LOADING)
	@ContextConfiguration(classes = Config.class)
	static class JdbcAggregateTemplateSingleQueryLoadingIntegrationTests
			extends AbstractJdbcAggregateTemplateIntegrationTests {

	}
}
