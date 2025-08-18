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
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Integration tests for {@link JdbcAggregateTemplate} and it's handling of entities with embedded entities as keys.
 *
 * @author Jens Schauder
 * @author Jaeyeon Kim
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
class CompositeIdAggregateTemplateHsqlIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired private NamedParameterJdbcOperations namedParameterJdbcTemplate;

	@Test // GH-574
	void saveAndLoadSimpleEntity() {

		SimpleEntity entity = template.insert(new SimpleEntity(new WrappedPk(23L), "alpha"));

		assertThat(entity.wrappedPk).isNotNull() //
				.extracting(WrappedPk::id).isNotNull();

		SimpleEntity reloaded = template.findById(entity.wrappedPk, SimpleEntity.class);

		assertThat(reloaded).isEqualTo(entity);
	}

	@Test // GH-574
	void saveAndLoadEntityWithList() {

		WithList entity = template
				.insert(new WithList(new WrappedPk(23L), "alpha", List.of(new Child("Romulus"), new Child("Remus"))));

		assertThat(entity.wrappedPk).isNotNull() //
				.extracting(WrappedPk::id).isNotNull();

		WithList reloaded = template.findById(entity.wrappedPk, WithList.class);

		assertThat(reloaded).isEqualTo(entity);
	}

	@Test // GH-574
	void saveAndLoadSimpleEntityWithEmbeddedPk() {

		SimpleEntityWithEmbeddedPk entity = template
				.insert(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"));

		SimpleEntityWithEmbeddedPk reloaded = template.findById(entity.embeddedPk, SimpleEntityWithEmbeddedPk.class);

		assertThat(reloaded).isEqualTo(entity);
	}

	@Test // GH-574
	void saveAndLoadSimpleEntitiesWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		List<EmbeddedPk> firstTwoPks = entities.stream().limit(2).map(SimpleEntityWithEmbeddedPk::embeddedPk).toList();
		Iterable<SimpleEntityWithEmbeddedPk> reloaded = template.findAllById(firstTwoPks, SimpleEntityWithEmbeddedPk.class);

		assertThat(reloaded).containsExactlyInAnyOrder(entities.get(0), entities.get(1));
	}

	@Test // GH-574
	void deleteSingleSimpleEntityWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		template.delete(entities.get(1));

		Iterable<SimpleEntityWithEmbeddedPk> reloaded = template.findAll(SimpleEntityWithEmbeddedPk.class);

		assertThat(reloaded).containsExactlyInAnyOrder(entities.get(0), entities.get(2));
	}

	@Test // GH-574
	void deleteMultipleSimpleEntityWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		template.deleteAll(List.of(entities.get(1), entities.get(0)));

		Iterable<SimpleEntityWithEmbeddedPk> reloaded = template.findAll(SimpleEntityWithEmbeddedPk.class);

		assertThat(reloaded).containsExactly(entities.get(2));
	}

	@Test // GH-1978
	void deleteAllByQueryWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(1L, "a"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(2L, "b"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(3L, "b"), "gamma")));

		Query query = Query.query(Criteria.where("name").is("beta"));
		template.deleteAllByQuery(query, SimpleEntityWithEmbeddedPk.class);

		assertThat(
				template.findAll(SimpleEntityWithEmbeddedPk.class))
				.containsExactlyInAnyOrder(
						entities.get(0), // alpha
						entities.get(2)  // gamma
				);
	}

	@Test // GH-574
	void existsSingleSimpleEntityWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		assertThat(template.existsById(entities.get(1).embeddedPk, SimpleEntityWithEmbeddedPk.class)).isTrue();
		assertThat(template.existsById(new EmbeddedPk(24L, "x"), SimpleEntityWithEmbeddedPk.class)).isFalse();

	}

	@Test // GH-574
	void updateSingleSimpleEntityWithEmbeddedPk() {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		SimpleEntityWithEmbeddedPk updated = new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "ALPHA");
		template.save(updated);

		Iterable<SimpleEntityWithEmbeddedPk> reloaded = template.findAll(SimpleEntityWithEmbeddedPk.class);

		assertThat(reloaded).containsExactlyInAnyOrder(updated, entities.get(1), entities.get(2));
	}

	@Test // GH-574
	void saveAndLoadSingleReferenceAggregate() {

		SingleReference entity = template.insert(new SingleReference(new EmbeddedPk(23L, "x"), "alpha", new Child("Alf")));

		SingleReference reloaded = template.findById(entity.embeddedPk, SingleReference.class);

		assertThat(reloaded).isEqualTo(entity);
	}

	@Test // GH-574
	void updateSingleReferenceAggregate() {

		EmbeddedPk id = new EmbeddedPk(23L, "x");
		template.insert(new SingleReference(id, "alpha", new Child("Alf")));

		SingleReference updated = new SingleReference(id, "beta", new Child("Barny"));
		template.save(updated);

		List<SingleReference> all = template.findAll(SingleReference.class);

		assertThat(all).containsExactly(updated);
	}

	@Test // GH-574
	void saveAndLoadWithListAndCompositeId() {

		WithListAndCompositeId entity = template.insert( //
				new WithListAndCompositeId( //
						new EmbeddedPk(23L, "x"), "alpha", //
						List.of( //
								new Child("Alf"), //
								new Child("Bob"), //
								new Child("Flo") //
						) //
				) //
		);

		WithListAndCompositeId reloaded = template.findById(entity.embeddedPk, WithListAndCompositeId.class);

		assertThat(reloaded).isEqualTo(entity);
	}

	@Test // GH-574
	void sortByCompositeIdParts() {

		SimpleEntityWithEmbeddedPk alpha = template.insert( //
				new SimpleEntityWithEmbeddedPk( //
						new EmbeddedPk(23L, "x"), "alpha" //
				));
		SimpleEntityWithEmbeddedPk bravo = template.insert( //
				new SimpleEntityWithEmbeddedPk( //
						new EmbeddedPk(22L, "a"), "bravo" //
				));
		SimpleEntityWithEmbeddedPk charlie = template.insert( //
				new SimpleEntityWithEmbeddedPk( //
						new EmbeddedPk(21L, "z"), "charlie" //
				) //
		);

		assertThat( //
				template.findAll(SimpleEntityWithEmbeddedPk.class, Sort.by("embeddedPk.one"))) //
				.containsExactly( //
						charlie, bravo, alpha //
				);

		assertThat( //
				template.findAll(SimpleEntityWithEmbeddedPk.class, Sort.by("embeddedPk.two").descending())) //
				.containsExactly( //
						charlie, alpha, bravo //
				);
	}

	@Test // GH-574
	void projectByCompositeIdParts() {

		template.insert( //
				new SimpleEntityWithEmbeddedPk( //
						new EmbeddedPk(23L, "x"), "alpha" //
				));

		Query projectingQuery = Query.empty().columns("embeddedPk.two", "name");
		SimpleEntityWithEmbeddedPk projected = template.findOne(projectingQuery, SimpleEntityWithEmbeddedPk.class)
				.orElseThrow();

		assertThat(projected).isEqualTo(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(null, "x"), "alpha"));

		projectingQuery = Query.empty().columns("embeddedPk", "name");
		projected = template.findOne(projectingQuery, SimpleEntityWithEmbeddedPk.class).orElseThrow();

		assertThat(projected).isEqualTo(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"));
	}

	private record WrappedPk(Long id) {
	}

	private record SimpleEntity( //
			@Id WrappedPk wrappedPk, //
			String name //
	) {
	}

	private record Child(String name) {
	}

	private record WithList( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) WrappedPk wrappedPk, //
			String name, List<Child> children) {
	}

	private record EmbeddedPk(Long one, String two) {
	}

	private record SimpleEntityWithEmbeddedPk( //
			@Id EmbeddedPk embeddedPk, //
			String name //
	) {
	}

	private record SingleReference( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) EmbeddedPk embeddedPk, //
			String name, //
			Child child) {
	}

	private record WithListAndCompositeId( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) EmbeddedPk embeddedPk, //
			String name, //
			List<Child> child) {
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return CompositeIdAggregateTemplateHsqlIntegrationTests.class;
		}

	}
}
