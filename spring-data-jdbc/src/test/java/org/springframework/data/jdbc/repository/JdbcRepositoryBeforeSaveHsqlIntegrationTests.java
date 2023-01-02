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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Value;
import lombok.With;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for the {@link BeforeSaveCallback}.
 *
 * @author Chirag Tailor
 */
@ContextConfiguration
@ExtendWith(SpringExtension.class)
@ActiveProfiles("hsql")
public class JdbcRepositoryBeforeSaveHsqlIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryBeforeSaveHsqlIntegrationTests.class;
		}
	}

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

	@Value
	@With
	static class ImmutableEntity {
		@Id Long id;
		String name;
	}

	private interface MutableEntityRepository extends ListCrudRepository<MutableEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class MutableEntity {
		@Id private Long id;
		private String name;
	}

	private interface MutableWithImmutableIdEntityRepository
			extends ListCrudRepository<MutableWithImmutableIdEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class MutableWithImmutableIdEntity {
		@Id private final Long id;
		private String name;
	}

	private interface ImmutableWithMutableIdEntityRepository
			extends ListCrudRepository<ImmutableWithMutableIdEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class ImmutableWithMutableIdEntity {
		@Id private Long id;
		@With private final String name;
	}

	@Configuration
	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = ListCrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryBeforeSaveHsqlIntegrationTests.class;
		}

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
