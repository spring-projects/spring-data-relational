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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.jdbc.testing.TestDatabaseFeatures.Feature.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Integration tests for {@link JdbcAggregateTemplate} using an entity mapped with an explicit schema.
 *
 * @author Jens Schauder
 */
@IntegrationTest
public class JdbcAggregateTemplateSchemaIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired NamedParameterJdbcOperations jdbcTemplate;

	@Test
	@EnabledOnFeature(SUPPORTS_QUOTED_IDS)
	public void insertFindUpdateDelete() {

		DummyEntity entity = new DummyEntity();
		entity.name = "Alfred";
		entity.reference = new Referenced();
		entity.reference.name = "Peter";

		template.save(entity);

		DummyEntity reloaded = template.findById(entity.id, DummyEntity.class);

		assertThat(reloaded).isNotNull();

		reloaded.name += " E. Neumann";

		template.save(reloaded);

		template.deleteById(reloaded.id, DummyEntity.class);
	}

	static class DummyEntity {

		@Id Long id;
		String name;
		Referenced reference;
	}

	static class Referenced {
		String name;
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return JdbcAggregateTemplateSchemaIntegrationTests.class;
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}

		@Bean
		NamingStrategy namingStrategy() {
			return new NamingStrategy() {
				@Override
				public String getSchema() {
					return "other";
				}
			};
		}
	}
}
