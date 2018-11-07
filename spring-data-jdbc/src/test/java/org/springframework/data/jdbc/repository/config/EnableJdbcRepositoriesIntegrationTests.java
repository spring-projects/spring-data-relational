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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import lombok.Data;

import java.lang.reflect.Field;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.MapperMap;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositoriesIntegrationTests.TestConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class EnableJdbcRepositoriesIntegrationTests {

	static final Field MAPPER_MAP = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class, "mapperMap");
	public static final RowMapper DUMMY_ENTITY_ROW_MAPPER = mock(RowMapper.class);
	public static final RowMapper STRING_ROW_MAPPER = mock(RowMapper.class);
	public static final ResultSetExtractor<Integer> INTEGER_RESULT_SET_EXTRACTOR = mock(ResultSetExtractor.class);

	@Autowired JdbcRepositoryFactoryBean factoryBean;
	@Autowired DummyRepository repository;

	@BeforeClass
	public static void setup() {
		MAPPER_MAP.setAccessible(true);
	}

	@Test // DATAJDBC-100
	public void repositoryGetsPickedUp() {

		assertThat(repository).isNotNull();

		Iterable<DummyEntity> all = repository.findAll();

		assertThat(all).isNotNull();
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorConfigurationGetsPickedUp() {
		MapperMap mapping = (MapperMap) ReflectionUtils.getField(MAPPER_MAP, factoryBean);
		assertThat(mapping.resultSetExtractorFor(Integer.class)).isEqualTo(INTEGER_RESULT_SET_EXTRACTOR);
	}
	
	@Test // DATAJDBC-290
	public void customResultSetExtractorConfigurationIsNotPickedUpIfRowMapperIsRegisteredForTheSameType() {
		MapperMap mapping = (MapperMap) ReflectionUtils.getField(MAPPER_MAP, factoryBean);
		assertThat(mapping.resultSetExtractorFor(String.class)).isNull();
	}
	
	@Test // DATAJDBC-166
	public void customRowMapperConfigurationGetsPickedUp() {

		MapperMap mapping = (MapperMap) ReflectionUtils.getField(MAPPER_MAP, factoryBean);

		assertThat(mapping.rowMapperFor(String.class)).isEqualTo(STRING_ROW_MAPPER);
		assertThat(mapping.rowMapperFor(DummyEntity.class)).isEqualTo(DUMMY_ENTITY_ROW_MAPPER);
	}

	interface DummyRepository extends CrudRepository<DummyEntity, Long> {

	}

	@Data
	static class DummyEntity {
		@Id private Long id;
	}

	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class))
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return EnableJdbcRepositoriesIntegrationTests.class;
		}

		@Bean
		MapperMap rowMappers() {
			return new ConfigurableMapperMap() //
					.registerRowMapper(DummyEntity.class, DUMMY_ENTITY_ROW_MAPPER) //
					.registerRowMapper(String.class, STRING_ROW_MAPPER)
					.registerResultSetExtractor(Integer.class, INTEGER_RESULT_SET_EXTRACTOR);
		}

	}
}
