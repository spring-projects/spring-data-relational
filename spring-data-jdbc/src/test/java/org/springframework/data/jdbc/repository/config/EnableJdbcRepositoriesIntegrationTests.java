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
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.InsertStrategyFactory;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.convert.SqlParametersFactory;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.ReflectionUtils;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Evgeni Dimitrov
 * @author Fei Dong
 * @author Chirag Tailor
 * @author Diego Krupitza
 */
@IntegrationTest
public class EnableJdbcRepositoriesIntegrationTests {

	static final Field MAPPER_MAP = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class,
			"queryMappingConfiguration");
	static final Field OPERATIONS = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class, "operations");
	static final Field DATA_ACCESS_STRATEGY = ReflectionUtils.findField(JdbcRepositoryFactoryBean.class,
			"dataAccessStrategy");
	public static final RowMapper DUMMY_ENTITY_ROW_MAPPER = mock(RowMapper.class);
	public static final RowMapper STRING_ROW_MAPPER = mock(RowMapper.class);

	@Autowired JdbcRepositoryFactoryBean factoryBean;
	@Autowired DummyRepository repository;
	@Autowired
	@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations defaultOperations;
	@Autowired
	@Qualifier("defaultDataAccessStrategy") DataAccessStrategy defaultDataAccessStrategy;
	@Autowired
	@Qualifier("qualifierJdbcOperations") NamedParameterJdbcOperations qualifierJdbcOperations;
	@Autowired
	@Qualifier("qualifierDataAccessStrategy") DataAccessStrategy qualifierDataAccessStrategy;

	@BeforeAll
	public static void setup() {

		MAPPER_MAP.setAccessible(true);
		OPERATIONS.setAccessible(true);
		DATA_ACCESS_STRATEGY.setAccessible(true);
	}

	@Test // DATAJDBC-100
	public void repositoryGetsPickedUp() {

		assertThat(repository).isNotNull();

		long count = repository.count();

		// the custom base class has a result of 23 hard wired.
		assertThat(count).isEqualTo(23L);
	}

	@Test // DATAJDBC-166
	public void customRowMapperConfigurationGetsPickedUp() {

		QueryMappingConfiguration mapping = (QueryMappingConfiguration) ReflectionUtils.getField(MAPPER_MAP, factoryBean);

		assertThat(mapping.getRowMapper(String.class)).isEqualTo(STRING_ROW_MAPPER);
		assertThat(mapping.getRowMapper(DummyEntity.class)).isEqualTo(DUMMY_ENTITY_ROW_MAPPER);
	}

	@Test // DATAJDBC-293
	public void jdbcOperationsRef() {

		NamedParameterJdbcOperations operations = (NamedParameterJdbcOperations) ReflectionUtils.getField(OPERATIONS,
				factoryBean);
		assertThat(operations).isNotSameAs(defaultOperations).isSameAs(qualifierJdbcOperations);

		DataAccessStrategy dataAccessStrategy = (DataAccessStrategy) ReflectionUtils.getField(DATA_ACCESS_STRATEGY,
				factoryBean);
		assertThat(dataAccessStrategy).isNotSameAs(defaultDataAccessStrategy).isSameAs(qualifierDataAccessStrategy);
	}

	interface DummyRepository extends CrudRepository<DummyEntity, Long> {

	}

	static class DummyEntity {
		@Id private Long id;

		public Long getId() {
			return this.id;
		}

		public void setId(Long id) {
			this.id = id;
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class),
			jdbcOperationsRef = "qualifierJdbcOperations", dataAccessStrategyRef = "qualifierDataAccessStrategy",
			repositoryBaseClass = DummyRepositoryBaseClass.class)
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		QueryMappingConfiguration rowMappers() {

			return new DefaultQueryMappingConfiguration() //
					.registerRowMapper(DummyEntity.class, DUMMY_ENTITY_ROW_MAPPER) //
					.registerRowMapper(String.class, STRING_ROW_MAPPER);
		}

		@Bean("qualifierJdbcOperations")
		NamedParameterJdbcOperations qualifierJdbcOperations(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
		}

		@Bean("qualifierDataAccessStrategy")
		DataAccessStrategy defaultDataAccessStrategy(
				@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template,
				RelationalMappingContext context, JdbcConverter converter, Dialect dialect) {
			return new DataAccessStrategyFactory(new SqlGeneratorSource(context, converter, dialect), converter, template,
					new SqlParametersFactory(context, converter),
					new InsertStrategyFactory(template, dialect)).create();
		}

		@Bean
		Dialect jdbcDialect(@Qualifier("qualifierJdbcOperations") NamedParameterJdbcOperations operations) {
			return DialectResolver.getDialect(operations.getJdbcOperations());
		}
	}

	private static class DummyRepositoryBaseClass<T, ID> implements CrudRepository<T, ID> {

		DummyRepositoryBaseClass(JdbcAggregateTemplate template, PersistentEntity<?, ?> persistentEntity,
				JdbcConverter converter) {

		}

		@Override
		public <S extends T> S save(S s) {
			return null;
		}

		@Override
		public <S extends T> Iterable<S> saveAll(Iterable<S> iterable) {
			return null;
		}

		@Override
		public Optional<T> findById(ID id) {
			return Optional.empty();
		}

		@Override
		public boolean existsById(ID id) {
			return false;
		}

		@Override
		public Iterable<T> findAll() {
			return null;
		}

		@Override
		public Iterable<T> findAllById(Iterable<ID> iterable) {
			return null;
		}

		@Override
		public long count() {
			return 23;
		}

		@Override
		public void deleteById(ID id) {

		}

		@Override
		public void delete(T t) {

		}

		@Override
		public void deleteAll(Iterable<? extends T> iterable) {

		}

		@Override
		public void deleteAll() {

		}

		@Override
		public void deleteAllById(Iterable<? extends ID> ids) {

		}
	}
}
