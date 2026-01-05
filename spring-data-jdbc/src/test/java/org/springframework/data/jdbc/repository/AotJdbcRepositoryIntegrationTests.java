/*
 * Copyright 2025-present the original author or authors.
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

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.repository.aot.AotFragmentTestConfigurationSupport;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.BeanFactoryAwareRowMapperFactory;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link DummyEntityRepository} using JavaConfig with mounted AOT-generated repository methods.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@IntegrationTest
@ContextConfiguration(classes = AotJdbcRepositoryIntegrationTests.AotConfig.class)
@EnabledOnDatabase(DatabaseType.H2)
class AotJdbcRepositoryIntegrationTests extends JdbcRepositoryIntegrationTests {

	@Configuration
	@EnableJdbcRepositories(includeFilters = {
			@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = AotJdbcRepositoryIntegrationTests.class) })
	static class AotConfig extends Config {

		@Autowired ApplicationContext context;

		@Bean
		TestClass testClass() {
			return TestClass.of(JdbcRepositoryIntegrationTests.class);
		}

		@Bean
		static AotFragmentTestConfigurationSupport aot() {
			return new AotFragmentTestConfigurationSupport(DummyEntityRepository.class, JdbcH2Dialect.INSTANCE, AotConfig.class,
					false);
		}

		@Bean
		BeanFactoryAwareRowMapperFactory rowMapperFactory(ApplicationContext context,
				JdbcAggregateOperations aggregateOperations, Optional<QueryMappingConfiguration> queryMappingConfiguration) {
			return new BeanFactoryAwareRowMapperFactory(context, aggregateOperations,
					queryMappingConfiguration.orElse(QueryMappingConfiguration.EMPTY));
		}

		@Bean
		@Override
		DummyEntityRepository dummyEntityRepository() {

			RepositoryComposition.RepositoryFragments fragments = RepositoryComposition.RepositoryFragments
					.just(context.getBean("fragment"));

			return factory.getRepository(DummyEntityRepository.class, fragments);
		}

	}

}
