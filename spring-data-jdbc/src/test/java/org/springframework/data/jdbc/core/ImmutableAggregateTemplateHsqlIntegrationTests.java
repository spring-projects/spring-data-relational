/*
 * Copyright 2017-2022 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;
import lombok.With;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for {@link JdbcAggregateTemplate} and it's handling of immutable entities.
 *
 * @author Jens Schauder
 * @author Salim Achouche
 * @author Chirag Tailor
 */
@ContextConfiguration
@Transactional
@ActiveProfiles("hsql")
@ExtendWith(SpringExtension.class)
public class ImmutableAggregateTemplateHsqlIntegrationTests {

	@Autowired JdbcAggregateOperations template;

	@Test // DATAJDBC-241
	public void saveWithGeneratedIdCreatesNewInstance() {

		LegoSet legoSet = createLegoSet(createManual());

		LegoSet saved = template.save(legoSet);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(legoSet).isNotSameAs(saved);
		softly.assertThat(legoSet.getId()).isNull();

		softly.assertThat(saved.getId()).isNotNull();
		softly.assertThat(saved.name).isNotNull();
		softly.assertThat(saved.manual).isNotNull();
		softly.assertThat(saved.manual.content).isNotNull();

		softly.assertAll();
	}

	@Test // DATAJDBC-241
	public void saveAndLoadAnEntityWithReferencedEntityById() {

		LegoSet saved = template.save(createLegoSet(createManual()));

		assertThat(saved.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNotNull();

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.getId()) //
				.isEqualTo(saved.getManual().getId()) //
				.isNotNull();
		softly.assertThat(reloadedLegoSet.manual.getContent()).isEqualTo(saved.getManual().getContent());

		softly.assertAll();
	}

	@Test // DATAJDBC-291
	public void saveAndLoadAnEntityWithTwoReferencedEntitiesById() {

		LegoSet saved = template.save(createLegoSet(createManual(), new Author(null, "Alfred E. Neumann")));

		assertThat(saved.manual.id).describedAs("id of stored manual").isNotNull();
		assertThat(saved.author.id).describedAs("id of stored author").isNotNull();

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNotNull();

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.getId()) //
				.isEqualTo(saved.getManual().getId()) //
				.isNotNull();
		softly.assertThat(reloadedLegoSet.manual.getContent()).isEqualTo(saved.getManual().getContent());
		softly.assertThat(reloadedLegoSet.author.getName()).isEqualTo(saved.getAuthor().getName());

		softly.assertAll();
	}

	@Test // DATAJDBC-241
	public void saveAndLoadManyEntitiesWithReferencedEntity() {

		LegoSet legoSet = createLegoSet(createManual());

		LegoSet savedLegoSet = template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(savedLegoSet.getId(), savedLegoSet.getManual().getId(), savedLegoSet.getManual().getContent()));
	}

	@Test // DATAJDBC-241
	public void saveAndLoadManyEntitiesByIdWithReferencedEntity() {

		LegoSet saved = template.save(createLegoSet(createManual()));

		Iterable<LegoSet> reloadedLegoSets = template.findAllById(singletonList(saved.getId()), LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(saved.getId(), saved.getManual().getId(), saved.getManual().getContent()));
	}

	@Test // DATAJDBC-241
	public void saveAndLoadAnEntityWithReferencedNullEntity() {

		LegoSet saved = template.save(createLegoSet(null));

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNull();
	}

	@Test // DATAJDBC-241
	public void saveAndDeleteAnEntityWithReferencedEntity() {

		LegoSet legoSet = createLegoSet(createManual());

		LegoSet saved = template.save(legoSet);

		template.delete(saved);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(template.findAll(LegoSet.class)).isEmpty();
		softly.assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-241
	public void saveAndDeleteAllWithReferencedEntity() {

		template.save(createLegoSet(createManual()));

		template.deleteAll(LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		assertThat(template.findAll(LegoSet.class)).isEmpty();
		assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-241
	public void updateReferencedEntityFromNull() {

		LegoSet saved = template.save(createLegoSet(null));

		LegoSet changedLegoSet = new LegoSet(saved.id, saved.name, new Manual(23L, "Some content"), null);

		template.save(changedLegoSet);

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("Some content");
	}

	@Test // DATAJDBC-241
	public void updateReferencedEntityToNull() {

		LegoSet saved = template.save(createLegoSet(null));

		LegoSet changedLegoSet = new LegoSet(saved.id, saved.name, null, null);

		template.save(changedLegoSet);

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual).isNull();
		softly.assertThat(template.findAll(Manual.class)).describedAs("Manuals failed to delete").isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-241
	public void replaceReferencedEntity() {

		LegoSet saved = template.save(createLegoSet(null));

		LegoSet changedLegoSet = new LegoSet(saved.id, saved.name, new Manual(null, "other content"), null);

		template.save(changedLegoSet);

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.content).isEqualTo("other content");
		softly.assertThat(template.findAll(Manual.class)).describedAs("There should be only one manual").hasSize(1);

		softly.assertAll();
	}

	@Test // GH-1201
	void replaceReferencedEntity_saveResult() {

		Root root = new Root(null, "originalRoot", new NonRoot(null, "originalNonRoot"));
		Root originalSavedRoot = template.save(root);

		assertThat(originalSavedRoot.id).isNotNull();
		assertThat(originalSavedRoot.name).isEqualTo("originalRoot");
		assertThat(originalSavedRoot.reference.id).isNotNull();
		assertThat(originalSavedRoot.reference.name).isEqualTo("originalNonRoot");

		Root updatedRoot = new Root(originalSavedRoot.id, "updatedRoot", new NonRoot(null, "updatedNonRoot"));
		Root updatedSavedRoot = template.save(updatedRoot);

		assertThat(updatedSavedRoot.id).isNotNull();
		assertThat(updatedSavedRoot.name).isEqualTo("updatedRoot");
		assertThat(updatedSavedRoot.reference.id).isNotNull().isNotEqualTo(originalSavedRoot.reference.id);
		assertThat(updatedSavedRoot.reference.name).isEqualTo("updatedNonRoot");
	}

	@Test // DATAJDBC-241
	public void changeReferencedEntity() {

		LegoSet saved = template.save(createLegoSet(createManual()));

		LegoSet changedLegoSet = saved.withManual(saved.manual.withContent("new content"));

		template.save(changedLegoSet);

		LegoSet reloadedLegoSet = template.findById(saved.getId(), LegoSet.class);

		Manual manual = reloadedLegoSet.manual;
		assertThat(manual).isNotNull();
		assertThat(manual.content).isEqualTo("new content");
	}

	@Test // DATAJDBC-545
	public void setIdViaConstructor() {

		WithCopyConstructor entity = new WithCopyConstructor(null, "Alfred");

		WithCopyConstructor saved = template.save(entity);

		assertThat(saved).isNotEqualTo(entity);
		assertThat(saved.id).isNotNull();
	}

	private static LegoSet createLegoSet(Manual manual) {

		return new LegoSet(null, "Star Destroyer", manual, null);
	}

	private static LegoSet createLegoSet(Manual manual, Author author) {

		return new LegoSet(null, "Star Destroyer", manual, author);
	}

	private static Manual createManual() {
		return new Manual(null,
				"Accelerates to 99% of light speed. Destroys almost everything. See https://what-if.xkcd.com/1/");
	}

	@Value
	@With
	static class LegoSet {

		@Id Long id;
		String name;
		Manual manual;
		Author author;
	}

	@Value
	@With
	static class Manual {

		@Id Long id;
		String content;
	}

	@Value
	@With
	static class Author {

		@Id Long id;
		String name;
	}

	@Data
	@AllArgsConstructor
	static class Root {
		@Id private Long id;
		private String name;
		private NonRoot reference;
	}

	@Value
	@With
	static class NonRoot {
		@Id Long id;
		String name;
	}

	static class WithCopyConstructor {
		@Id private final Long id;
		private final String name;

		WithCopyConstructor(Long id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return ImmutableAggregateTemplateHsqlIntegrationTests.class;
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}
}
