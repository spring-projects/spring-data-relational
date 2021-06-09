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
package org.springframework.data.jdbc.core;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.With;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

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
 */
@ContextConfiguration
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class JdbcAggregateTemplateIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired NamedParameterJdbcOperations jdbcTemplate;

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
		entity.setName(name);

		Manual manual = new Manual();
		manual.setContent("Accelerates to 99% of light speed. Destroys almost everything. See https://what-if.xkcd.com/1/");
		entity.setManual(manual);

		return entity;
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadAnEntityWithReferencedEntityById() {

		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNotNull();

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.getId()) //
				.isEqualTo(legoSet.getManual().getId()) //
				.isNotNull();
		softly.assertThat(reloadedLegoSet.manual.getContent()).isEqualTo(legoSet.getManual().getContent());

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadManyEntitiesWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class);

		assertThat(reloadedLegoSets) //
				.extracting("id", "manual.id", "manual.content") //
				.containsExactly(tuple(legoSet.getId(), legoSet.getManual().getId(), legoSet.getManual().getContent()));
	}

	@Test // DATAJDBC-101
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadManyEntitiesWithReferencedEntitySorted() {

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
	public void saveAndLoadManyEntitiesWithReferencedEntitySortedAndPaged() {

		template.save(createLegoSet("Lava"));
		template.save(createLegoSet("Star"));
		template.save(createLegoSet("Frozen"));

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class, PageRequest.of(1, 2, Sort.by("name")));

		assertThat(reloadedLegoSets) //
				.extracting("name") //
				.containsExactly("Star");
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadManyEntitiesByIdWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAllById(singletonList(legoSet.getId()), LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(legoSet.getId(), legoSet.getManual().getId(), legoSet.getManual().getContent()));
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadAnEntityWithReferencedNullEntity() {

		legoSet.setManual(null);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNull();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndDeleteAnEntityWithReferencedEntity() {

		template.save(legoSet);

		template.delete(legoSet, LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(template.findAll(LegoSet.class)).isEmpty();
		softly.assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndDeleteAllWithReferencedEntity() {

		template.save(legoSet);

		template.deleteAll(LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		assertThat(template.findAll(LegoSet.class)).isEmpty();
		assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS, SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES })
	public void updateReferencedEntityFromNull() {

		legoSet.setManual(null);
		template.save(legoSet);

		Manual manual = new Manual();
		manual.setId(23L);
		manual.setContent("Some content");
		legoSet.setManual(manual);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("Some content");
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void updateReferencedEntityToNull() {

		template.save(legoSet);

		legoSet.setManual(null);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual).isNull();
		softly.assertThat(template.findAll(Manual.class)).describedAs("Manuals failed to delete").isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-438
	public void updateFailedRootDoesNotExist() {

		LegoSet entity = new LegoSet();
		entity.setId(100L); // does not exist in the database

		assertThatExceptionOfType(DbActionExecutionException.class) //
				.isThrownBy(() -> template.save(entity)) //
				.withCauseInstanceOf(IncorrectUpdateSemanticsDataAccessException.class);
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void replaceReferencedEntity() {

		template.save(legoSet);

		Manual manual = new Manual();
		manual.setContent("other content");
		legoSet.setManual(manual);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.content).isEqualTo("other content");
		softly.assertThat(template.findAll(Manual.class)).describedAs("There should be only one manual").hasSize(1);

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	@EnabledOnFeature({ SUPPORTS_QUOTED_IDS, TestDatabaseFeatures.Feature.SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES })
	public void changeReferencedEntity() {

		template.save(legoSet);

		legoSet.manual.setContent("new content");

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("new content");
	}

	@Test // DATAJDBC-266
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void oneToOneChildWithoutId() {

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
	public void oneToOneNullChildWithoutId() {

		OneToOneParent parent = new OneToOneParent();

		parent.content = "parent content";
		parent.child = null;

		template.save(parent);

		OneToOneParent reloaded = template.findById(parent.id, OneToOneParent.class);

		assertThat(reloaded.child).isNull();
	}

	@Test // DATAJDBC-266
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void oneToOneNullAttributes() {

		OneToOneParent parent = new OneToOneParent();

		parent.content = "parent content";
		parent.child = new ChildNoId();

		template.save(parent);

		OneToOneParent reloaded = template.findById(parent.id, OneToOneParent.class);

		assertThat(reloaded.child).isNotNull();
	}

	@Test // DATAJDBC-125
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadAnEntityWithSecondaryReferenceNull() {

		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.alternativeInstructions).isNull();
	}

	@Test // DATAJDBC-125
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadAnEntityWithSecondaryReferenceNotNull() {

		legoSet.alternativeInstructions = new Manual();
		legoSet.alternativeInstructions.content = "alternative content";
		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(reloadedLegoSet.alternativeInstructions).isNotNull();
		softly.assertThat(reloadedLegoSet.alternativeInstructions.id).isNotNull();
		softly.assertThat(reloadedLegoSet.alternativeInstructions.id).isNotEqualTo(reloadedLegoSet.manual.id);
		softly.assertThat(reloadedLegoSet.alternativeInstructions.content)
				.isEqualTo(reloadedLegoSet.alternativeInstructions.content);

		softly.assertAll();
	}

	@Test // DATAJDBC-276
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadAnEntityWithListOfElementsWithoutId() {

		ListParent entity = new ListParent();
		entity.name = "name";

		ElementNoId element = new ElementNoId();
		element.content = "content";

		entity.content.add(element);

		template.save(entity);

		ListParent reloaded = template.findById(entity.id, ListParent.class);

		assertThat(reloaded.content).extracting(e -> e.content).containsExactly("content");
	}

	@Test // DATAJDBC-259
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	public void saveAndLoadAnEntityWithArray() {

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
	public void saveAndLoadAnEntityWithMultidimensionalArray() {

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
	public void saveAndLoadAnEntityWithList() {

		ListOwner arrayOwner = new ListOwner();
		arrayOwner.digits.addAll(Arrays.asList("one", "two", "three"));

		ListOwner saved = template.save(arrayOwner);

		assertThat(saved.id).isNotNull();

		ListOwner reloaded = template.findById(saved.id, ListOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(Arrays.asList("one", "two", "three"));
	}

	@Test // DATAJDBC-259
	@EnabledOnFeature(SUPPORTS_ARRAYS)
	public void saveAndLoadAnEntityWithSet() {

		SetOwner setOwner = new SetOwner();
		setOwner.digits.addAll(Arrays.asList("one", "two", "three"));

		SetOwner saved = template.save(setOwner);

		assertThat(saved.id).isNotNull();

		SetOwner reloaded = template.findById(saved.id, SetOwner.class);

		assertThat(reloaded).isNotNull();
		assertThat(reloaded.id).isEqualTo(saved.id);
		assertThat(reloaded.digits).isEqualTo(new HashSet<>(Arrays.asList("one", "two", "three")));
	}

	@Test // DATAJDBC-327
	public void saveAndLoadAnEntityWithByteArray() {

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
	public void saveAndLoadLongChain() {

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

		template.delete(chain4, Chain4.class);

		assertThat(count("CHAIN0")).isEqualTo(0);
	}

	@Test // DATAJDBC-359
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void saveAndLoadLongChainWithoutIds() {

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

		template.delete(chain4, NoIdChain4.class);

		assertThat(count("CHAIN0")).isEqualTo(0);
	}

	@Test // DATAJDBC-223
	public void saveAndLoadLongChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());

		assertThat(saved.four).describedAs("Something went wrong during saving").isNotNull();

		NoIdListChain4 reloaded = template.findById(saved.four, NoIdListChain4.class);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test // DATAJDBC-223
	public void shouldDeleteChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());
		template.deleteById(saved.four, NoIdListChain4.class);

		assertSoftly(softly -> {

			softly.assertThat(count("NO_ID_LIST_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
		});
	}

	@Test // DATAJDBC-223
	public void saveAndLoadLongChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());

		assertThat(saved.four).isNotNull();

		NoIdMapChain4 reloaded = template.findById(saved.four, NoIdMapChain4.class);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test // DATAJDBC-223
	public void shouldDeleteChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());
		template.deleteById(saved.four, NoIdMapChain4.class);

		assertSoftly(softly -> {

			softly.assertThat(count("NO_ID_MAP_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
		});
	}

	@Test // DATAJDBC-431
	@EnabledOnFeature(IS_HSQL)
	public void readOnlyGetsLoadedButNotWritten() {

		WithReadOnly entity = new WithReadOnly();
		entity.name = "Alfred";
		entity.readOnly = "not used";

		template.save(entity);

		assertThat(
				jdbcTemplate.queryForObject("SELECT read_only FROM with_read_only", Collections.emptyMap(), String.class))
						.isEqualTo("from-db");
	}

	@Test // DATAJDBC-219 Test that immutable version attribute works as expected.
	public void saveAndUpdateAggregateWithImmutableVersion() {

		AggregateWithImmutableVersion aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);
		assertThat(aggregate.version).isEqualTo(0L);

		Long id = aggregate.getId();

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
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);

		assertThatThrownBy(() -> template.save(new AggregateWithImmutableVersion(id, 2L)))
				.describedAs("saving an aggregate with a future version should raise an exception")
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATAJDBC-219 Test that a delete with a version attribute works as expected.
	public void deleteAggregateWithVersion() {

		AggregateWithImmutableVersion aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);
		// as non-primitive versions start from 0, we need to save one more time to make version equal 1
		aggregate = template.save(aggregate);

		// Should have an ID and a version of 1.
		final Long id = aggregate.getId();

		assertThatThrownBy(
				() -> template.delete(new AggregateWithImmutableVersion(id, 0L), AggregateWithImmutableVersion.class))
						.describedAs("deleting an aggregate with an outdated version should raise an exception")
						.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);

		assertThatThrownBy(
				() -> template.delete(new AggregateWithImmutableVersion(id, 2L), AggregateWithImmutableVersion.class))
						.describedAs("deleting an aggregate with a future version should raise an exception")
						.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);

		// This should succeed
		template.delete(aggregate, AggregateWithImmutableVersion.class);

		aggregate = new AggregateWithImmutableVersion(null, null);
		aggregate = template.save(aggregate);

		// This should succeed, as version will not be used.
		template.deleteById(aggregate.getId(), AggregateWithImmutableVersion.class);

	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithLongVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithLongVersion(), Number::longValue);
	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithPrimitiveLongVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveLongVersion(), Number::longValue);
	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithIntegerVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithIntegerVersion(), Number::intValue);
	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithPrimitiveIntegerVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveIntegerVersion(), Number::intValue);
	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithShortVersion() {
		saveAndUpdateAggregateWithVersion(new AggregateWithShortVersion(), Number::shortValue);
	}

	@Test // DATAJDBC-219
	public void saveAndUpdateAggregateWithPrimitiveShortVersion() {
		saveAndUpdateAggregateWithPrimitiveVersion(new AggregateWithPrimitiveShortVersion(), Number::shortValue);
	}

	@Test // DATAJDBC-462
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void resavingAnUnversionedEntity() {

		LegoSet legoSet = new LegoSet();

		LegoSet saved = template.save(legoSet);

		template.save(saved);
	}

	@Test // DATAJDBC-637
	@EnabledOnFeature(SUPPORTS_NANOSECOND_PRECISION)
	public void saveAndLoadDateTimeWithFullPrecision() {

		WithLocalDateTime entity = new WithLocalDateTime();
		entity.id = 23L;
		entity.testTime = LocalDateTime.of(2005, 5, 5, 5, 5, 5, 123456789);

		template.insert(entity);

		WithLocalDateTime loaded = template.findById(23L, WithLocalDateTime.class);

		assertThat(loaded.testTime).isEqualTo(entity.testTime);
	}

	@Test // DATAJDBC-637
	public void saveAndLoadDateTimeWithMicrosecondPrecision() {

		WithLocalDateTime entity = new WithLocalDateTime();
		entity.id = 23L;
		entity.testTime = LocalDateTime.of(2005, 5, 5, 5, 5, 5, 123456000);

		template.insert(entity);

		WithLocalDateTime loaded = template.findById(23L, WithLocalDateTime.class);

		assertThat(loaded.testTime).isEqualTo(entity.testTime);
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

		VersionedAggregate reloadedAggregate = template.findById(aggregate.getId(), aggregate.getClass());
		assertThat(reloadedAggregate.getVersion()) //
				.withFailMessage("version field should initially have the value 0")
				.isEqualTo(toConcreteNumber.apply(initialId));
		template.save(reloadedAggregate);

		VersionedAggregate updatedAggregate = template.findById(aggregate.getId(), aggregate.getClass());
		assertThat(updatedAggregate.getVersion()) //
				.withFailMessage("version field should increment by one with each save")
				.isEqualTo(toConcreteNumber.apply(initialId + 1));

		reloadedAggregate.setVersion(toConcreteNumber.apply(initialId));
		assertThatThrownBy(() -> template.save(reloadedAggregate))
				.withFailMessage("saving an aggregate with an outdated version should raise an exception")
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);

		reloadedAggregate.setVersion(toConcreteNumber.apply(initialId + 2));
		assertThatThrownBy(() -> template.save(reloadedAggregate))
				.withFailMessage("saving an aggregate with a future version should raise an exception")
				.hasRootCauseInstanceOf(OptimisticLockingFailureException.class);
	}

	private Long count(String tableName) {
		return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, emptyMap(), Long.class);
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

	@Data
	static class LegoSet {

		@Column("id1") @Id private Long id;

		private String name;

		private Manual manual;
		@Column("alternative") private Manual alternativeInstructions;
	}

	@Data
	static class Manual {

		@Column("id2") @Id private Long id;
		private String content;

	}

	static class OneToOneParent {

		@Column("id3") @Id private Long id;
		private String content;

		private ChildNoId child;
	}

	static class ChildNoId {
		private String content;
	}

	static class ListParent {

		@Column("id4") @Id private Long id;
		String name;
		List<ElementNoId> content = new ArrayList<>();
	}

	static class ElementNoId {
		private String content;
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	static class Chain0 {
		@Id Long zero;
		String zeroValue;
	}

	static class Chain1 {
		@Id Long one;
		String oneValue;
		Chain0 chain0;
	}

	static class Chain2 {
		@Id Long two;
		String twoValue;
		Chain1 chain1;
	}

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
	@EqualsAndHashCode
	static class NoIdListChain0 {
		String zeroValue;
	}

	@EqualsAndHashCode
	static class NoIdListChain1 {
		String oneValue;
		List<NoIdListChain0> chain0 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain2 {
		String twoValue;
		List<NoIdListChain1> chain1 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain3 {
		String threeValue;
		List<NoIdListChain2> chain2 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain4 {
		@Id Long four;
		String fourValue;
		List<NoIdListChain3> chain3 = new ArrayList<>();
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	@EqualsAndHashCode
	static class NoIdMapChain0 {
		String zeroValue;
	}

	@EqualsAndHashCode
	static class NoIdMapChain1 {
		String oneValue;
		Map<String, NoIdMapChain0> chain0 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain2 {
		String twoValue;
		Map<String, NoIdMapChain1> chain1 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain3 {
		String threeValue;
		Map<String, NoIdMapChain2> chain2 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain4 {
		@Id Long four;
		String fourValue;
		Map<String, NoIdMapChain3> chain3 = new HashMap<>();
	}

	static class WithReadOnly {
		@Id Long id;
		String name;
		@ReadOnlyProperty String readOnly;
	}

	@Data
	static abstract class VersionedAggregate {

		@Id private Long id;

		abstract Number getVersion();

		abstract void setVersion(Number newVersion);
	}

	@Value
	@With
	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithImmutableVersion {

		@Id Long id;
		@Version Long version;
	}

	@Data
	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithLongVersion extends VersionedAggregate {

		@Version private Long version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Long) newVersion;
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

	@Data
	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithIntegerVersion extends VersionedAggregate {

		@Version private Integer version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Integer) newVersion;
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

	@Data
	@Table("VERSIONED_AGGREGATE")
	static class AggregateWithShortVersion extends VersionedAggregate {

		@Version private Short version;

		@Override
		void setVersion(Number newVersion) {
			this.version = (Short) newVersion;
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

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return JdbcAggregateTemplateIntegrationTests.class;
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}
}
