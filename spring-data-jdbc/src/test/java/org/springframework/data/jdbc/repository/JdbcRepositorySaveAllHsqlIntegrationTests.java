/*
 * Copyright 2022 the original author or authors.
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
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

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
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import lombok.Value;

/**
 * Use cases of JdbcRepositories where operations are carried out across multiple aggregate roots at a time.
 *
 * @author Chirag Tailor
 */
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
@ActiveProfiles("hsql")
public class JdbcRepositorySaveAllHsqlIntegrationTests {

	@Autowired RootRepository rootRepository;

	@Test
	void manyInsertsWithNestedEntities() {
		Root root1 = createRoot("root1");
		Root root2 = createRoot("root2");

		List<Root> savedRoots = rootRepository.saveAll(asList(root1, root2));

		List<Root> reloadedRoots = rootRepository.findAll();
		assertThat(reloadedRoots).isEqualTo(savedRoots);
		assertThat(reloadedRoots).hasSize(2);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(0), root1);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(1), root2);
	}

	@Test
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

		List<Root> reloadedRoots = rootRepository.findAll();
		assertThat(reloadedRoots).isEqualTo(updatedRoots);
		assertThat(reloadedRoots).containsExactly(updatedRoot1, updatedRoot2);
	}

	@Test
	void manyInsertsAndUpdatedWithNesteEntities() {
		Root root1 = createRoot("root1");
		Root savedRoot1 = rootRepository.save(root1);
		Root updatedRoot1 = new Root(savedRoot1.id, "updated" + savedRoot1.name,
				new Intermediate(savedRoot1.intermediate.id, "updated" + savedRoot1.intermediate.name,
						new Leaf(savedRoot1.intermediate.leaf.id, "updated" + savedRoot1.intermediate.leaf.name), emptyList()),
				savedRoot1.intermediates);
		Root root2 = createRoot("root2");
		List<Root> savedRoots = rootRepository.saveAll(asList(updatedRoot1, root2));

		List<Root> reloadedRoots = rootRepository.findAll();
		assertThat(reloadedRoots).isEqualTo(savedRoots);
		assertThat(reloadedRoots.get(0)).isEqualTo(updatedRoot1);
		assertIsEqualToWithNonNullIds(reloadedRoots.get(1), root2);
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

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositorySaveAllHsqlIntegrationTests.class;
		}

		@Bean
		RootRepository rootRepository() {
			return factory.getRepository(RootRepository.class);
		}
	}

	interface RootRepository extends ListCrudRepository<Root, Long> {}

	@Value
	static class Root {
		@Id Long id;
		String name;
		Intermediate intermediate;
		@MappedCollection(idColumn = "ROOT_ID", keyColumn = "ROOT_KEY") List<Intermediate> intermediates;
	}

	@Value
	static class Intermediate {
		@Id Long id;
		String name;
		Leaf leaf;
		@MappedCollection(idColumn = "INTERMEDIATE_ID", keyColumn = "INTERMEDIATE_KEY") List<Leaf> leaves;
	}

	@Value
	static class Leaf {
		@Id Long id;
		String name;
	}
}
