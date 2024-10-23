/*
 * Copyright 2017-2024 the original author or authors.
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
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;

import junit.framework.AssertionFailedError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Very simple use cases for creation and usage of JdbcRepositories for Entities that contain {@link List}s.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Chirag Tailor
 */
@IntegrationTest
public class JdbcRepositoryWithListsIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired RootRepository rootRepository;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity(null, "Entity Name", new ArrayList<>());
		return entity;
	}

	@Test // DATAJDBC-130
	public void saveAndLoadEmptyList() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(entity.id).isNotNull();

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);

		assertThat(reloaded.content) //
				.isNotNull() //
				.isEmpty();
	}

	@Test // DATAJDBC-130
	public void saveAndLoadNonEmptyList() {

		Element element1 = new Element();
		Element element2 = new Element();

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		assertThat(entity.id).isNotNull();
		assertThat(entity.content).allMatch(v -> v.id != null);

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);

		assertThat(reloaded.content) //
				.isNotNull() //
				.extracting(e -> e.id) //
				.containsExactlyInAnyOrder(entity.content.get(0).id, entity.content.get(1).id);
	}

	@Test // GH-1159
	void saveAndLoadNonEmptyNestedList() {
		Root root = new Root();
		Intermediate intermediate1 = new Intermediate();
		root.intermediates.add(intermediate1);
		Intermediate intermediate2 = new Intermediate();
		root.intermediates.add(intermediate2);
		Leaf leaf1 = new Leaf("leaf1");
		Leaf leaf2 = new Leaf("leaf2");
		intermediate1.leaves.addAll(asList(leaf1, leaf2));
		Leaf leaf3 = new Leaf("leaf3");
		Leaf leaf4 = new Leaf("leaf4");
		intermediate2.leaves.addAll(asList(leaf3, leaf4));

		rootRepository.save(root);

		assertThat(root.id).isNotNull();
		assertThat(root.intermediates).allMatch(v -> v.id != null);

		Root reloaded = rootRepository.findById(root.id).orElseThrow(AssertionFailedError::new);
		assertThat(reloaded.intermediates.get(0).leaves).containsExactly(leaf1, leaf2);
		assertThat(reloaded.intermediates.get(1).leaves).containsExactly(leaf3, leaf4);
	}

	@Test // DATAJDBC-130
	public void findAllLoadsList() {

		Element element1 = new Element();
		Element element2 = new Element();

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		assertThat(entity.id).isNotNull();
		assertThat(entity.content).allMatch(v -> v.id != null);

		Iterable<DummyEntity> reloaded = repository.findAll();

		assertThat(reloaded) //
				.extracting(e -> e.id, e -> e.content.size()) //
				.containsExactly(tuple(entity.id, entity.content.size()));
	}

	@Test // DATAJDBC-130
	@EnabledOnFeature(SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES)
	public void updateList() {

		Element element1 = new Element("one");
		Element element2 = new Element("two");
		Element element3 = new Element("three");

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		entity.content.remove(0);
		entity.content.set(0, new Element(entity.content.get(0).id, "two changed"));
		entity.content.add(element3);

		entity = repository.save(entity);

		assertThat(entity.id).isNotNull();
		assertThat(entity.content).allMatch(v -> v.id != null);
		assertThat(entity.content).hasSize(2);

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);

		// the elements got properly updated and reloaded
		assertThat(reloaded.content) //
				.isNotNull();

		assertThat(reloaded.content) //
				.extracting(e -> e.id, e -> e.content) //
				.containsExactly( //
						tuple(entity.content.get(0).id, "two changed"), //
						tuple(entity.content.get(1).id, "three") //
				);

		Long count = template.queryForObject("SELECT count(1) FROM Element", new HashMap<>(), Long.class);
		assertThat(count).isEqualTo(2);
	}

	@Test // DATAJDBC-130
	public void deletingWithList() {

		Element element1 = new Element("one");
		Element element2 = new Element("two");

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		repository.deleteById(entity.id);

		assertThat(repository.findById(entity.id)).isEmpty();

		Long count = template.queryForObject("SELECT count(1) FROM Element", new HashMap<>(), Long.class);
		assertThat(count).isEqualTo(0);
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	interface RootRepository extends CrudRepository<Root, Long> {}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		DummyEntityRepository dummyEntityRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		RootRepository rootRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(RootRepository.class);
		}
	}

	record DummyEntity(@Id Long id, String name, List<Element> content) {
	}

	record Element(@Id Long id, String content) {

		@PersistenceCreator
		Element {}

		Element() {
			this(null, null);
		}

		Element(String content) {
			this(null, content);
		}

	}

	static class Root {
		@Id private Long id;
		List<Intermediate> intermediates = new ArrayList<>();

		public Long getId() {
			return this.id;
		}

		public List<Intermediate> getIntermediates() {
			return this.intermediates;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setIntermediates(List<Intermediate> intermediates) {
			this.intermediates = intermediates;
		}
	}

	static class Intermediate {
		@Id private Long id;
		List<Leaf> leaves = new ArrayList<>();

		public Long getId() {
			return this.id;
		}

		public List<Leaf> getLeaves() {
			return this.leaves;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setLeaves(List<Leaf> leaves) {
			this.leaves = leaves;
		}
	}

	static final class Leaf {
		private final String name;

		public Leaf(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Leaf other))
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
			return "JdbcRepositoryWithListsIntegrationTests.Leaf(name=" + this.getName() + ")";
		}
	}
}
