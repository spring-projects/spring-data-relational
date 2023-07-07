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
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import junit.framework.AssertionFailedError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories for Entities that contain {@link List}s.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Chirag Tailor
 */
@ContextConfiguration
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryWithListsIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired RootRepository rootRepository;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setName("Entity Name");
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
				.containsExactlyInAnyOrder(element1.id, element2.id);
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

		Element element1 = createElement("one");
		Element element2 = createElement("two");
		Element element3 = createElement("three");

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		entity.content.remove(element1);
		element2.content = "two changed";
		entity.content.add(element3);

		entity = repository.save(entity);

		assertThat(entity.id).isNotNull();
		assertThat(entity.content).allMatch(v -> v.id != null);

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);

		// the elements got properly updated and reloaded
		assertThat(reloaded.content) //
				.isNotNull();

		assertThat(reloaded.content) //
				.extracting(e -> e.id, e -> e.content) //
				.containsExactly( //
						tuple(element2.id, "two changed"), //
						tuple(element3.id, "three") //
				);

		Long count = template.queryForObject("SELECT count(1) FROM Element", new HashMap<>(), Long.class);
		assertThat(count).isEqualTo(2);
	}

	@Test // DATAJDBC-130
	public void deletingWithList() {

		Element element1 = createElement("one");
		Element element2 = createElement("two");

		DummyEntity entity = createDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = repository.save(entity);

		repository.deleteById(entity.id);

		assertThat(repository.findById(entity.id)).isEmpty();

		Long count = template.queryForObject("SELECT count(1) FROM Element", new HashMap<>(), Long.class);
		assertThat(count).isEqualTo(0);
	}

	private Element createElement(String content) {

		Element element = new Element();
		element.content = content;
		return element;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	interface RootRepository extends CrudRepository<Root, Long> {}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryWithListsIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		RootRepository rootRepository() {
			return factory.getRepository(RootRepository.class);
		}
	}

	static class DummyEntity {

		String name;
		List<Element> content = new ArrayList<>();
		@Id private Long id;

		public String getName() {
			return this.name;
		}

		public List<Element> getContent() {
			return this.content;
		}

		public Long getId() {
			return this.id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setContent(List<Element> content) {
			this.content = content;
		}

		public void setId(Long id) {
			this.id = id;
		}
	}

	static class Element {

		String content;
		@Id private Long id;

		public Element() {}
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
			if (!(o instanceof Leaf))
				return false;
			final Leaf other = (Leaf) o;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (this$name == null ? other$name != null : !this$name.equals(other$name))
				return false;
			return true;
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
