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

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Objects;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Integration tests for {@link JdbcAggregateTemplate} and it's handling of immutable entities.
 *
 * @author Jens Schauder
 * @author Salim Achouche
 * @author Chirag Tailor
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
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

	static final class LegoSet {

		@Id private final Long id;
		private final String name;
		private final Manual manual;
		private final Author author;

		public LegoSet(Long id, String name, Manual manual, Author author) {
			this.id = id;
			this.name = name;
			this.manual = manual;
			this.author = author;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Manual getManual() {
			return this.manual;
		}

		public Author getAuthor() {
			return this.author;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final LegoSet other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$manual = this.getManual();
			final Object other$manual = other.getManual();
			if (!Objects.equals(this$manual, other$manual))
				return false;
			final Object this$author = this.getAuthor();
			final Object other$author = other.getAuthor();
			return Objects.equals(this$author, other$author);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $manual = this.getManual();
			result = result * PRIME + ($manual == null ? 43 : $manual.hashCode());
			final Object $author = this.getAuthor();
			result = result * PRIME + ($author == null ? 43 : $author.hashCode());
			return result;
		}

		public String toString() {
			return "ImmutableAggregateTemplateHsqlIntegrationTests.LegoSet(id=" + this.getId() + ", name=" + this.getName()
					+ ", manual=" + this.getManual() + ", author=" + this.getAuthor() + ")";
		}

		public LegoSet withId(Long id) {
			return this.id == id ? this : new LegoSet(id, this.name, this.manual, this.author);
		}

		public LegoSet withName(String name) {
			return this.name == name ? this : new LegoSet(this.id, name, this.manual, this.author);
		}

		public LegoSet withManual(Manual manual) {
			return this.manual == manual ? this : new LegoSet(this.id, this.name, manual, this.author);
		}

		public LegoSet withAuthor(Author author) {
			return this.author == author ? this : new LegoSet(this.id, this.name, this.manual, author);
		}
	}

	static final class Manual {

		@Id private final Long id;
		private final String content;

		public Manual(Long id, String content) {
			this.id = id;
			this.content = content;
		}

		public Long getId() {
			return this.id;
		}

		public String getContent() {
			return this.content;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Manual other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$content = this.getContent();
			final Object other$content = other.getContent();
			return Objects.equals(this$content, other$content);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $content = this.getContent();
			result = result * PRIME + ($content == null ? 43 : $content.hashCode());
			return result;
		}

		public String toString() {
			return "ImmutableAggregateTemplateHsqlIntegrationTests.Manual(id=" + this.getId() + ", content="
					+ this.getContent() + ")";
		}

		public Manual withId(Long id) {
			return this.id == id ? this : new Manual(id, this.content);
		}

		public Manual withContent(String content) {
			return this.content == content ? this : new Manual(this.id, content);
		}
	}

	static final class Author {

		@Id private final Long id;
		private final String name;

		public Author(Long id, String name) {
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
			if (!(o instanceof final Author other))
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
			return "ImmutableAggregateTemplateHsqlIntegrationTests.Author(id=" + this.getId() + ", name=" + this.getName()
					+ ")";
		}

		public Author withId(Long id) {
			return this.id == id ? this : new Author(id, this.name);
		}

		public Author withName(String name) {
			return this.name == name ? this : new Author(this.id, name);
		}
	}

	static class Root {
		@Id private Long id;
		private String name;
		private NonRoot reference;

		public Root(Long id, String name, NonRoot reference) {
			this.id = id;
			this.name = name;
			this.reference = reference;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public NonRoot getReference() {
			return this.reference;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setReference(NonRoot reference) {
			this.reference = reference;
		}
	}

	static final class NonRoot {
		@Id private final Long id;
		private final String name;

		public NonRoot(Long id, String name) {
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
			if (!(o instanceof final NonRoot other))
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
			return "ImmutableAggregateTemplateHsqlIntegrationTests.NonRoot(id=" + this.getId() + ", name=" + this.getName()
					+ ")";
		}

		public NonRoot withId(Long id) {
			return this.id == id ? this : new NonRoot(id, this.name);
		}

		public NonRoot withName(String name) {
			return this.name == name ? this : new NonRoot(this.id, name);
		}
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
