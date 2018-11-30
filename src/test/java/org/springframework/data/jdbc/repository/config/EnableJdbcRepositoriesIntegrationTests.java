/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import javax.sql.DataSource;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositoriesIntegrationTests.TestConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import lombok.Data;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class EnableJdbcRepositoriesIntegrationTests {

	static final Field ROW_MAPPER_MAP = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class, "rowMapperMap");
	static final Field OPERATIONS = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class, "operations");
	static final Field DATA_ACCESS_STRATEGY = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class, "dataAccessStrategy");
	public static final RowMapper DUMMY_ENTITY_ROW_MAPPER = mock(RowMapper.class);
	public static final RowMapper STRING_ROW_MAPPER = mock(RowMapper.class);

	@Autowired JdbcRepositoryFactoryBean factoryBean;
	@Autowired DummyRepository repository;
	@Autowired @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations defaultOperations;
	@Autowired @Qualifier("defaultDataAccessStrategy") DataAccessStrategy defaultDataAccessStrategy;
	@Autowired @Qualifier("qualifierJdbcOperations") NamedParameterJdbcOperations qualifierJdbcOperations;
	@Autowired @Qualifier("qualifierDataAccessStrategy") DataAccessStrategy qualifierDataAccessStrategy;

	@BeforeClass
	public static void setup() {
		ROW_MAPPER_MAP.setAccessible(true);
		OPERATIONS.setAccessible(true);
		DATA_ACCESS_STRATEGY.setAccessible(true);
	}

	@Test // DATAJDBC-100
	public void repositoryGetsPickedUp() {

		assertThat(repository).isNotNull();

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all).isNotNull();
	}

	@Test // DATAJDBC-166
	public void customRowMapperConfigurationGetsPickedUp() {

		RowMapperMap mapping = (RowMapperMap) ReflectionUtils.getField(ROW_MAPPER_MAP, factoryBean);

		assertThat(mapping.rowMapperFor(String.class)).isEqualTo(STRING_ROW_MAPPER);
		assertThat(mapping.rowMapperFor(DummyEntity.class)).isEqualTo(DUMMY_ENTITY_ROW_MAPPER);
	}

 	@Test // DATAJDBC-293
	public void jdbcOperationsRef() {
		NamedParameterJdbcOperations operations = (NamedParameterJdbcOperations) ReflectionUtils.getField(OPERATIONS, factoryBean);
		assertThat(operations).isNotSameAs(defaultDataAccessStrategy).isSameAs(qualifierJdbcOperations);

		DataAccessStrategy dataAccessStrategy = (DataAccessStrategy) ReflectionUtils.getField(DATA_ACCESS_STRATEGY, factoryBean);
		assertThat(dataAccessStrategy).isNotSameAs(defaultDataAccessStrategy).isSameAs(qualifierDataAccessStrategy);
	}

	interface DummyRepository extends CrudRepository<DummyEntity, Long> {

	}

	@Data
	static class DummyEntity {
		@Id private Long id;
	}

	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true, 
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class), 
			jdbcOperationsRef = "qualifierJdbcOperations", dataAccessStrategyRef = "qualifierDataAccessStrategy")
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return EnableJdbcRepositoriesIntegrationTests.class;
		}

		@Bean
		RowMapperMap rowMappers() {
			return new ConfigurableRowMapperMap() //
					.register(DummyEntity.class, DUMMY_ENTITY_ROW_MAPPER) //
					.register(String.class, STRING_ROW_MAPPER);
		}

		@Bean("qualifierJdbcOperations")
		NamedParameterJdbcOperations qualifierJdbcOperations(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
		}

		@Bean("qualifierDataAccessStrategy")
		DataAccessStrategy defaultDataAccessStrategy(@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template,
				RelationalMappingContext context, RelationalConverter converter) {
			return new DefaultDataAccessStrategy(new SqlGeneratorSource(context), context, converter, template);
		}
	}
}
