/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.DialectResolver;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests the {@link EnableJdbcRepositories} annotation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Evgeni Dimitrov
 * @author Fei Dong
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @author Mikhail Polivakha
 * @author Mark Paluch
 */
class EnableJdbcRepositoriesIntegrationTests {

	private static final RowMapper DUMMY_ENTITY_ROW_MAPPER = mock(RowMapper.class);
	private static final RowMapper STRING_ROW_MAPPER = mock(RowMapper.class);


	@Test // DATAJDBC-100
	void repositoryGetsPickedUp() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(DefaultConfig.class)) {

			DummyRepository repository = context.getBean(DummyRepository.class);

			long count = repository.count();

			// the custom base class has a result of 23 hard wired.
			assertThat(count).isEqualTo(23L);
		}
	}

	@Test // DATAJDBC-166
	void customRowMapperConfigurationGetsPickedUp() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ConfigWithQualifier.class)) {

			JdbcRepositoryFactoryBean factoryBean = context.getBean(JdbcRepositoryFactoryBean.class);

			QueryMappingConfiguration mapping = (QueryMappingConfiguration) ReflectionTestUtils.getField(factoryBean,
					"queryMappingConfiguration");

		assertThat(mapping.getRowMapper(String.class)).isEqualTo(STRING_ROW_MAPPER);
		assertThat(mapping.getRowMapper(DummyEntity.class)).isEqualTo(DUMMY_ENTITY_ROW_MAPPER);
	}

	}

	@Test  // GH-1704
	void jdbcAggregateOperationsRef() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ConfigWithQualifier.class)) {

			JdbcAggregateOperations operations = context.getBean("qualifierJdbcAggregateOperations",
					JdbcAggregateOperations.class);
			JdbcAggregateOperations jdbcAggregateOperations = context.getBean("jdbcAggregateOperations",
					JdbcAggregateOperations.class);
			JdbcRepositoryFactoryBean factoryBean = context.getBean(JdbcRepositoryFactoryBean.class);

			assertThat(factoryBean).extracting("aggregateOperations").isEqualTo(operations)
					.isNotEqualTo(jdbcAggregateOperations);
		}
	}

	@Test // GH-1704
	void jdbcOperationsRef() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ConfigWithJdbcOperations.class)) {

			DataAccessStrategy dataAccessStrategy = context.getBean(DataAccessStrategy.class);

			JdbcAggregateOperations jdbcAggregateOperations = context.getBean(JdbcAggregateOperations.class);
			JdbcRepositoryFactoryBean factoryBean = context.getBean(JdbcRepositoryFactoryBean.class);

			assertThat(factoryBean).extracting("dataAccessStrategy").isEqualTo(dataAccessStrategy);
			assertThat(factoryBean).extracting("aggregateOperations").isNotEqualTo(jdbcAggregateOperations);
		}
	}

	@Test // GH-1704
	void dataAccessStrategyRef() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ConfigWithDataAccessStrategy.class)) {

			DataAccessStrategy dataAccessStrategy = context.getBean("qualifierDataAccessStrategy", DataAccessStrategy.class);

			JdbcAggregateOperations jdbcAggregateOperations = context.getBean(JdbcAggregateOperations.class);
			JdbcRepositoryFactoryBean factoryBean = context.getBean(JdbcRepositoryFactoryBean.class);

			assertThat(factoryBean).extracting("dataAccessStrategy").isEqualTo(dataAccessStrategy);
			assertThat(factoryBean).extracting("aggregateOperations").isNotEqualTo(jdbcAggregateOperations);
		}
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
			repositoryBaseClass = DummyRepositoryBaseClass.class)
	@Import(TestConfiguration.class)
	static class DefaultConfig {

		@Bean
		TestClass testClass() {
			return TestClass.of(EnableJdbcRepositoriesIntegrationTests.class);
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class),
			jdbcAggregateOperationsRef = "qualifierJdbcAggregateOperations")
	@Import(TestConfiguration.class)
	static class ConfigWithQualifier {

		@Bean
		TestClass testClass() {
			return TestClass.of(EnableJdbcRepositoriesIntegrationTests.class);
		}

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
		DataAccessStrategy qualifierDataAccessStrategy(
				@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template, JdbcConverter converter,
				Dialect dialect) {
			return new DataAccessStrategyFactory(converter, template, dialect,
					QueryMappingConfiguration.EMPTY).create();
		}

		@Bean
		Dialect jdbcDialect(@Qualifier("qualifierJdbcOperations") NamedParameterJdbcOperations operations) {
			return DialectResolver.getDialect(operations.getJdbcOperations());
		}

		@Bean("qualifierJdbcAggregateOperations")
		JdbcAggregateOperations qualifierJdbcAggregateOperations(JdbcConverter converter,
				@Qualifier("qualifierDataAccessStrategy") DataAccessStrategy dataAccessStrategy) {
			return new JdbcAggregateTemplate(converter, dataAccessStrategy);
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class),
			dataAccessStrategyRef = "qualifierDataAccessStrategy", jdbcOperationsRef = "qualifierJdbcOperations")
	@Import(TestConfiguration.class)
	static class ConfigWithDataAccessStrategy {

		@Bean
		TestClass testClass() {
			return TestClass.of(EnableJdbcRepositoriesIntegrationTests.class);
		}


		@Bean("qualifierJdbcOperations")
		NamedParameterJdbcOperations qualifierJdbcOperations(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
		}

		@Bean("qualifierDataAccessStrategy")
		DataAccessStrategy qualifierDataAccessStrategy(
				@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template,
				JdbcConverter converter, Dialect dialect) {
			return new DataAccessStrategyFactory(converter, template, dialect, QueryMappingConfiguration.EMPTY).create();
		}

		@Bean
		Dialect jdbcDialect(@Qualifier("qualifierJdbcOperations") NamedParameterJdbcOperations operations) {
			return DialectResolver.getDialect(operations.getJdbcOperations());
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = DummyRepository.class),
			jdbcOperationsRef = "qualifierJdbcOperations")
	@Import(TestConfiguration.class)
	static class ConfigWithJdbcOperations {

		@Bean
		TestClass testClass() {
			return TestClass.of(EnableJdbcRepositoriesIntegrationTests.class);
		}

		@Bean("qualifierJdbcOperations")
		NamedParameterJdbcOperations qualifierJdbcOperations(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
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
