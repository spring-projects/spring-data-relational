/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Integration tests for entity callbacks used with repositories that are not backed by a
 * {@code JdbcAggregateOperations} bean, i.e. configured through {@link EnableJdbcRepositories#dataAccessStrategyRef()}
 * or created through a hand-crafted {@link JdbcRepositoryFactory}.
 *
 * @author Jens Schauder
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
public class JdbcRepositoryEntityCallbacksHsqlIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired CallbackEntityRepository repository;
	@Autowired JdbcConverter converter;
	@Autowired DataAccessStrategy dataAccessStrategy;

	@Test // GH-2392
	void beforeConvertCallbackGetsApplied() {

		CallbackEntity saved = repository.save(new CallbackEntity(null, "initial"));

		assertThat(saved.getName()).isEqualTo("fromBeforeConvertCallback");
		assertThat(template.queryForObject("SELECT NAME FROM CallbackEntity", Collections.emptyMap(), String.class))
				.isEqualTo("fromBeforeConvertCallback");
	}

	@Test // GH-2392
	void afterConvertCallbackGetsApplied() {

		CallbackEntity saved = repository.save(new CallbackEntity(null, "initial"));

		assertThat(repository.findById(saved.getId())).get().extracting(CallbackEntity::getName)
				.isEqualTo("fromAfterConvertCallback");
	}

	interface CallbackEntityRepository extends ListCrudRepository<CallbackEntity, Long> {}

	static class CallbackEntity {

		@Id private Long id;
		private String name;

		CallbackEntity(Long id, String name) {

			this.id = id;
			this.name = name;
		}

		Long getId() {
			return id;
		}

		String getName() {
			return name;
		}

		void setName(String name) {
			this.name = name;
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CallbackEntityRepository.class, type = FilterType.ASSIGNABLE_TYPE),
			dataAccessStrategyRef = "defaultDataAccessStrategy")
	@Import(TestConfiguration.class)
	static class Config {

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
		BeforeConvertCallback<CallbackEntity> beforeConvertNameSetter() {

			return aggregate -> {

				aggregate.setName("fromBeforeConvertCallback");
				return aggregate;
			};
		}

		@Bean
		AfterConvertCallback<CallbackEntity> afterConvertNameSetter() {

			return aggregate -> {

				aggregate.setName("fromAfterConvertCallback");
				return aggregate;
			};
		}
	}
}
