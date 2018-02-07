/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests the execution of queries from {@link Query} annotations on repository methods.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
public class QueryAnnotationHsqlIntegrationTests {

	@Autowired DummyEntityRepository repository;

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Test // DATAJDBC-164
	public void executeCustomQueryWithoutParameter() {

		repository.save(dummyEntity("Example"));
		repository.save(dummyEntity("example"));
		repository.save(dummyEntity("EXAMPLE"));

		List<DummyEntity> entities = repository.findByNameContainingCapitalLetter();

		assertThat(entities) //
			.extracting(e -> e.name) //
			.containsExactlyInAnyOrder("Example", "EXAMPLE");

	}

	@Test // DATAJDBC-164
	public void executeCustomQueryWithNamedParameters() {

		repository.save(dummyEntity("a"));
		repository.save(dummyEntity("b"));
		repository.save(dummyEntity("c"));

		List<DummyEntity> entities = repository.findByNamedRangeWithNamedParameter("a", "c");

		assertThat(entities) //
			.extracting(e -> e.name) //
			.containsExactlyInAnyOrder("b");

	}

	@Test // DATAJDBC-164
	public void executeCustomQueryWithReturnTypeIsOptional() {

		DummyEntity dummyEntity = dummyEntity("a");
		repository.save(dummyEntity);

		Optional<DummyEntity> entity = repository.findByIdWithReturnTypeIsOptional(dummyEntity.id);

		assertThat(entity).map(e -> e.name).contains("a");

	}

	@Test // DATAJDBC-164
	public void executeCustomQueryWithReturnTypeIsEntity() {

		DummyEntity dummyEntity = dummyEntity("a");
		repository.save(dummyEntity);

		DummyEntity entity = repository.findByIdWithReturnTypeIsEntity(dummyEntity.id);

		assertThat(entity).isNotNull();
		assertThat(entity.name).isEqualTo("a");

	}

	@Test // DATAJDBC-164
	public void executeCustomQueryWithReturnTypeIsStream() {

		repository.save(dummyEntity("a"));
		repository.save(dummyEntity("b"));

		Stream<DummyEntity> entities = repository.findAllWithReturnTypeIsStream();

		assertThat(entities) //
			.extracting(e -> e.name) //
			.containsExactlyInAnyOrder("a", "b");

	}

	private DummyEntity dummyEntity(String name) {

		DummyEntity entity = new DummyEntity();
		entity.name = name;
		return entity;
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		@Bean
		Class<?> testClass() {
			return QueryAnnotationHsqlIntegrationTests.class;
		}
	}

	private static class DummyEntity {

		@Id Long id;

		String name;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

		@Query("SELECT * FROM DUMMYENTITY WHERE lower(name) <> name")
		List<DummyEntity> findByNameContainingCapitalLetter();

		@Query("SELECT * FROM DUMMYENTITY WHERE name  < :upper and name > :lower")
		List<DummyEntity> findByNamedRangeWithNamedParameter(@Param("lower") String lower, @Param("upper") String upper);
		
		@Query("SELECT * FROM DUMMYENTITY WHERE id = :id FOR UPDATE")
		Optional<DummyEntity> findByIdWithReturnTypeIsOptional(@Param("id") Long id);

		@Query("SELECT * FROM DUMMYENTITY WHERE id = :id FOR UPDATE")
		DummyEntity findByIdWithReturnTypeIsEntity(@Param("id") Long id);

		@Query("SELECT * FROM DUMMYENTITY")
		Stream<DummyEntity> findAllWithReturnTypeIsStream();

	}
}
