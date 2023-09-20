/*
 * Copyright 2022-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Integration tests for the {@link BeforeSaveCallback}.
 *
 * @author Chirag Tailor
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
public class JdbcRepositoryBeforeSaveHsqlIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired ImmutableEntityRepository immutableWithManualIdEntityRepository;
	@Autowired MutableEntityRepository mutableEntityRepository;
	@Autowired MutableWithImmutableIdEntityRepository mutableWithImmutableIdEntityRepository;
	@Autowired ImmutableWithMutableIdEntityRepository immutableWithMutableIdEntityRepository;

	@Test // GH-1199
	public void immutableEntity() {

		ImmutableEntity entity = new ImmutableEntity(null, "immutable");
		ImmutableEntity saved = immutableWithManualIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		List<ImmutableEntity> entities = immutableWithManualIdEntityRepository.findAll();
		assertThat(entities).hasSize(1);
		ImmutableEntity reloaded = entities.get(0);
		assertThat(reloaded.getId()).isNotNull();
		assertThat(reloaded.getName()).isEqualTo("fromBeforeSaveCallback");
	}

	@Test // GH-1199
	public void mutableEntity() {

		MutableEntity entity = new MutableEntity(null, "immutable");
		MutableEntity saved = mutableEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		List<MutableEntity> entities = mutableEntityRepository.findAll();
		assertThat(entities).hasSize(1);
		MutableEntity reloaded = entities.get(0);
		assertThat(reloaded.getId()).isNotNull();
		assertThat(reloaded.getName()).isEqualTo("fromBeforeSaveCallback");
	}

	@Test // GH-1199
	public void mutableWithImmutableIdEntity() {

		MutableWithImmutableIdEntity entity = new MutableWithImmutableIdEntity(null, "immutable");
		MutableWithImmutableIdEntity saved = mutableWithImmutableIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		List<MutableWithImmutableIdEntity> entities = mutableWithImmutableIdEntityRepository.findAll();
		assertThat(entities).hasSize(1);
		MutableWithImmutableIdEntity reloaded = entities.get(0);
		assertThat(reloaded.getId()).isNotNull();
		assertThat(reloaded.getName()).isEqualTo("fromBeforeSaveCallback");
	}

	@Test // GH-1199
	public void immutableWithMutableIdEntity() {

		ImmutableWithMutableIdEntity entity = new ImmutableWithMutableIdEntity(null, "immutable");
		ImmutableWithMutableIdEntity saved = immutableWithMutableIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		List<ImmutableWithMutableIdEntity> entities = immutableWithMutableIdEntityRepository.findAll();
		assertThat(entities).hasSize(1);
		ImmutableWithMutableIdEntity reloaded = entities.get(0);
		assertThat(reloaded.getId()).isNotNull();
		assertThat(reloaded.getName()).isEqualTo("fromBeforeSaveCallback");
	}

	private interface ImmutableEntityRepository extends ListCrudRepository<ImmutableEntity, Long> {}

	static final class ImmutableEntity {
		@Id private final Long id;
		private final String name;

		public ImmutableEntity(Long id, String name) {
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
			if (!(o instanceof final ImmutableEntity other))
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
			return "JdbcRepositoryBeforeSaveHsqlIntegrationTests.ImmutableEntity(id=" + this.getId() + ", name="
					+ this.getName() + ")";
		}

		public ImmutableEntity withId(Long id) {
			return this.id == id ? this : new ImmutableEntity(id, this.name);
		}

		public ImmutableEntity withName(String name) {
			return this.name == name ? this : new ImmutableEntity(this.id, name);
		}
	}

	private interface MutableEntityRepository extends ListCrudRepository<MutableEntity, Long> {}

	static class MutableEntity {
		@Id private Long id;
		private String name;

		public MutableEntity(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	private interface MutableWithImmutableIdEntityRepository
			extends ListCrudRepository<MutableWithImmutableIdEntity, Long> {}

	static class MutableWithImmutableIdEntity {
		@Id private final Long id;
		private String name;

		public MutableWithImmutableIdEntity(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	private interface ImmutableWithMutableIdEntityRepository
			extends ListCrudRepository<ImmutableWithMutableIdEntity, Long> {}

	static class ImmutableWithMutableIdEntity {
		@Id private Long id;
		private final String name;

		public ImmutableWithMutableIdEntity(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public ImmutableWithMutableIdEntity withName(String name) {
			return this.name == name ? this : new ImmutableWithMutableIdEntity(this.id, name);
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = ListCrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	@Import(TestConfiguration.class)
	static class Config {

		/**
		 * {@link NamingStrategy} that harmlessly uppercases the table name, demonstrating how to inject one while not
		 * breaking existing SQL operations.
		 */
		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				@Override
				public String getTableName(Class<?> type) {
					return type.getSimpleName().toUpperCase();
				}
			};
		}

		@Bean
		BeforeSaveCallback<ImmutableEntity> nameSetterImmutable() {
			return (aggregate, aggregateChange) -> aggregate.withName("fromBeforeSaveCallback");
		}

		@Bean
		BeforeSaveCallback<MutableEntity> nameSetterMutable() {
			return (aggregate, aggregateChange) -> {
				aggregate.setName("fromBeforeSaveCallback");
				return aggregate;
			};
		}

		@Bean
		BeforeSaveCallback<MutableWithImmutableIdEntity> nameSetterMutableWithImmutableId() {
			return (aggregate, aggregateChange) -> {
				aggregate.setName("fromBeforeSaveCallback");
				return aggregate;
			};
		}

		@Bean
		BeforeSaveCallback<ImmutableWithMutableIdEntity> nameSetterImmutableWithMutableId() {
			return (aggregate, aggregateChange) -> aggregate.withName("fromBeforeSaveCallback");
		}
	}
}
