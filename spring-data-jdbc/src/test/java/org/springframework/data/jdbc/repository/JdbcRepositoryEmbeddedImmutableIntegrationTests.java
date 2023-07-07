/*
 * Copyright 2019-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.*;

/**
 * Very simple use cases for creation and usage of JdbcRepositories with {@link Embedded} annotation in Entities.
 *
 * @author Bastian Wilhelm
 */
@ContextConfiguration
@Transactional
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryEmbeddedImmutableIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryEmbeddedImmutableIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;

	@Test // DATAJDBC-111
	public void saveAndLoadAnEntity() {

		DummyEntity entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getId())).hasValueSatisfying(it -> {
			assertThat(it.getId()).isEqualTo(entity.getId());
			assertThat(it.getPrefixedEmbeddable().getAttr1()).isEqualTo(entity.getPrefixedEmbeddable().getAttr1());
			assertThat(it.getPrefixedEmbeddable().getAttr2()).isEqualTo(entity.getPrefixedEmbeddable().getAttr2());
		});
	}

	private static DummyEntity createDummyEntity() {
		return new DummyEntity(null, new Embeddable(1L, "test1"));
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	static final class DummyEntity {

		@Id
		private final Long id;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "PREFIX_")
		private final Embeddable prefixedEmbeddable;

		public DummyEntity(Long id, Embeddable prefixedEmbeddable) {
			this.id = id;
			this.prefixedEmbeddable = prefixedEmbeddable;
		}

		public Long getId() {
			return this.id;
		}

		public Embeddable getPrefixedEmbeddable() {
			return this.prefixedEmbeddable;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof DummyEntity)) return false;
			final DummyEntity other = (DummyEntity) o;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$prefixedEmbeddable = this.getPrefixedEmbeddable();
			final Object other$prefixedEmbeddable = other.getPrefixedEmbeddable();
			if (this$prefixedEmbeddable == null ? other$prefixedEmbeddable != null : !this$prefixedEmbeddable.equals(other$prefixedEmbeddable))
				return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $prefixedEmbeddable = this.getPrefixedEmbeddable();
			result = result * PRIME + ($prefixedEmbeddable == null ? 43 : $prefixedEmbeddable.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryEmbeddedImmutableIntegrationTests.DummyEntity(id=" + this.getId() + ", prefixedEmbeddable=" + this.getPrefixedEmbeddable() + ")";
		}

		public DummyEntity withId(Long id) {
			return this.id == id ? this : new DummyEntity(id, this.prefixedEmbeddable);
		}

		public DummyEntity withPrefixedEmbeddable(Embeddable prefixedEmbeddable) {
			return this.prefixedEmbeddable == prefixedEmbeddable ? this : new DummyEntity(this.id, prefixedEmbeddable);
		}
	}

	private static final class Embeddable {

		private final Long attr1;
		private final String attr2;

		public Embeddable(Long attr1, String attr2) {
			this.attr1 = attr1;
			this.attr2 = attr2;
		}

		public Long getAttr1() {
			return this.attr1;
		}

		public String getAttr2() {
			return this.attr2;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Embeddable)) return false;
			final Embeddable other = (Embeddable) o;
			final Object this$attr1 = this.getAttr1();
			final Object other$attr1 = other.getAttr1();
			if (this$attr1 == null ? other$attr1 != null : !this$attr1.equals(other$attr1)) return false;
			final Object this$attr2 = this.getAttr2();
			final Object other$attr2 = other.getAttr2();
			if (this$attr2 == null ? other$attr2 != null : !this$attr2.equals(other$attr2)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $attr1 = this.getAttr1();
			result = result * PRIME + ($attr1 == null ? 43 : $attr1.hashCode());
			final Object $attr2 = this.getAttr2();
			result = result * PRIME + ($attr2 == null ? 43 : $attr2.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcRepositoryEmbeddedImmutableIntegrationTests.Embeddable(attr1=" + this.getAttr1() + ", attr2=" + this.getAttr2() + ")";
		}

		public Embeddable withAttr1(Long attr1) {
			return this.attr1 == attr1 ? this : new Embeddable(attr1, this.attr2);
		}

		public Embeddable withAttr2(String attr2) {
			return this.attr2 == attr2 ? this : new Embeddable(this.attr1, attr2);
		}
	}
}
