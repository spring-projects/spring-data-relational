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

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Testing special cases for id generation with {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@IntegrationTest
class JdbcRepositoryIdGenerationIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired ReadOnlyIdEntityRepository readOnlyIdRepository;
	@Autowired PrimitiveIdEntityRepository primitiveIdRepository;
	@Autowired ImmutableWithManualIdEntityRepository immutableWithManualIdEntityRepository;

	@Test // DATAJDBC-98
	void idWithoutSetterGetsSet() {

		ReadOnlyIdEntity entity = readOnlyIdRepository.save(new ReadOnlyIdEntity(null, "Entity Name"));

		assertThat(entity.id()).isNotNull();

		assertThat(readOnlyIdRepository.findById(entity.id())).hasValueSatisfying(it -> {

			assertThat(it.id()).isEqualTo(entity.id());
			assertThat(it.name()).isEqualTo(entity.name());
		});
	}

	@Test // DATAJDBC-98
	void primitiveIdGetsSet() {

		PrimitiveIdEntity entity = new PrimitiveIdEntity();
		entity.setName("Entity Name");

		PrimitiveIdEntity saved = primitiveIdRepository.save(entity);

		assertThat(saved.getId()).isNotEqualTo(0L);

		assertThat(primitiveIdRepository.findById(saved.getId())).hasValueSatisfying(it -> {

			assertThat(it.getId()).isEqualTo(saved.getId());
			assertThat(it.getName()).isEqualTo(saved.getName());
		});
	}

	@Test // DATAJDBC-393
	void manuallyGeneratedId() {

		ImmutableWithManualIdEntity entity = new ImmutableWithManualIdEntity(null, "immutable");
		ImmutableWithManualIdEntity saved = immutableWithManualIdEntityRepository.save(entity);

		assertThat(saved.id()).isNotNull();

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(1);
	}

	@Test // DATAJDBC-393
	void manuallyGeneratedIdForSaveAll() {

		ImmutableWithManualIdEntity one = new ImmutableWithManualIdEntity(null, "one");
		ImmutableWithManualIdEntity two = new ImmutableWithManualIdEntity(null, "two");
		List<ImmutableWithManualIdEntity> saved = immutableWithManualIdEntityRepository.saveAll(List.of(one, two));

		assertThat(saved).allSatisfy(e -> assertThat(e.id).isNotNull());

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(2);
	}

	private interface PrimitiveIdEntityRepository extends ListCrudRepository<PrimitiveIdEntity, Long> {}

	private interface ReadOnlyIdEntityRepository extends ListCrudRepository<ReadOnlyIdEntity, Long> {}

	private interface ImmutableWithManualIdEntityRepository extends ListCrudRepository<ImmutableWithManualIdEntity, Long> {}

	record ReadOnlyIdEntity(@Id Long id, String name) {
	}

	static class PrimitiveIdEntity {

		@Id private long id;
		String name;

		public long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	record ImmutableWithManualIdEntity(@Id Long id, String name) {

		@Override
		public Long id() {
				return this.id;
			}

			public ImmutableWithManualIdEntity withId(Long id) {
				return this.id == id ? this : new ImmutableWithManualIdEntity(id, this.name);
			}

			public ImmutableWithManualIdEntity withName(String name) {
				return this.name == name ? this : new ImmutableWithManualIdEntity(this.id, name);
			}
		}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	@Import(TestConfiguration.class)
	static class Config {

		AtomicLong lastId = new AtomicLong(0);

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
		BeforeConvertCallback<ImmutableWithManualIdEntity> idGenerator() {
			return e -> e.withId(lastId.incrementAndGet());
		}
	}
}
