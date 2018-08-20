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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import lombok.Value;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.lang.Nullable;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests the execution of queries from {@link Query} annotations on repository methods.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
@ContextConfiguration
@ActiveProfiles("hsql")
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

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsOptional() {

		repository.save(dummyEntity("a"));

		Optional<DummyEntity> entity = repository.findByNameAsOptional("a");

		assertThat(entity).map(e -> e.name).contains("a");

	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsOptionalWhenEntityNotFound() {

		repository.save(dummyEntity("a"));

		Optional<DummyEntity> entity = repository.findByNameAsOptional("x");

		assertThat(entity).isNotPresent();

	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsEntity() {

		repository.save(dummyEntity("a"));

		DummyEntity entity = repository.findByNameAsEntity("a");

		assertThat(entity).isNotNull();
		assertThat(entity.name).isEqualTo("a");

	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsEntityWhenEntityNotFound() {

		repository.save(dummyEntity("a"));

		DummyEntity entity = repository.findByNameAsEntity("x");

		assertThat(entity).isNull();

	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsEntityWhenEntityDuplicateResult() {

		repository.save(dummyEntity("a"));
		repository.save(dummyEntity("a"));

		assertThatExceptionOfType(DataAccessException.class) //
				.isThrownBy(() -> repository.findByNameAsEntity("a"));
	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsOptionalWhenEntityDuplicateResult() {

		repository.save(dummyEntity("a"));
		repository.save(dummyEntity("a"));

		assertThatExceptionOfType(DataAccessException.class) //
				.isThrownBy(() -> repository.findByNameAsOptional("a"));
	}

	@Test // DATAJDBC-172
	public void executeCustomQueryWithReturnTypeIsStream() {

		repository.save(dummyEntity("a"));
		repository.save(dummyEntity("b"));

		Stream<DummyEntity> entities = repository.findAllWithReturnTypeIsStream();

		assertThat(entities) //
				.extracting(e -> e.name) //
				.containsExactlyInAnyOrder("a", "b");

	}

	@Test // DATAJDBC-175
	public void executeCustomQueryWithReturnTypeIsNubmer() {

		repository.save(dummyEntity("aaa"));
		repository.save(dummyEntity("bbb"));
		repository.save(dummyEntity("cac"));

		int count = repository.countByNameContaining("a");

		assertThat(count).isEqualTo(2);

	}

	@Test // DATAJDBC-175
	public void executeCustomQueryWithReturnTypeIsBoolean() {

		repository.save(dummyEntity("aaa"));
		repository.save(dummyEntity("bbb"));
		repository.save(dummyEntity("cac"));

		assertThat(repository.existsByNameContaining("a")).isTrue();
		assertThat(repository.existsByNameContaining("d")).isFalse();

	}

	@Test // DATAJDBC-175
	public void executeCustomQueryWithReturnTypeIsDate() {

		// Since Timestamp extends Date the repository returns the Timestamp as it comes from the database.
		// Trying to compare that to an actual Date results in non determistic results, so we have to use an actual
		// Timestamp.
		Date now = new Timestamp(System.currentTimeMillis());
		assertThat(repository.nowWithDate()).isAfterOrEqualsTo(now);

	}

	@Test // DATAJDBC-175
	public void executeCustomQueryWithReturnTypeIsLocalDateTimeList() {

		LocalDateTime now = LocalDateTime.now();
		repository.nowWithLocalDateTimeList() //
				.forEach(d -> assertThat(d).isAfterOrEqualTo(now));

	}

	@Test // DATAJDBC-182
	public void executeCustomModifyingQueryWithReturnTypeNumber() {

		DummyEntity entity = dummyEntity("a");
		repository.save(entity);

		assertThat(repository.updateName(entity.id, "b")).isEqualTo(1);
		assertThat(repository.updateName(9999L, "c")).isEqualTo(0);

		assertThat(repository.findById(entity.id)) //
				.describedAs("update was not performed as expected") //
				.isPresent() //
				.map(e -> e.name).contains("b");
	}

	@Test // DATAJDBC-182
	public void executeCustomModifyingQueryWithReturnTypeBoolean() {

		DummyEntity entity = dummyEntity("a");
		repository.save(entity);

		assertThat(repository.deleteByName("a")).isTrue();
		assertThat(repository.deleteByName("b")).isFalse();

		assertThat(repository.findById(entity.id)) //
				.describedAs("delete not performed as expected") //
				.isNotPresent();
	}

	@Test // DATAJDBC-182
	public void executeCustomModifyingQueryWithReturnTypeVoid() {

		repository.insert("Spring Data JDBC");

		assertThat(repository.findByNameAsEntity("Spring Data JDBC")).isNotNull();
	}

	@Test // DATAJDBC-175
	public void executeCustomQueryWithImmutableResultType() {

		assertThat(repository.immutableTuple()).isEqualTo(new DummyEntityRepository.ImmutableTuple("one", "two", 3));
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

		// DATAJDBC-164
		@Query("SELECT * FROM DUMMY_ENTITY WHERE lower(name) <> name")
		List<DummyEntity> findByNameContainingCapitalLetter();

		// DATAJDBC-164
		@Query("SELECT * FROM DUMMY_ENTITY WHERE name  < :upper and name > :lower")
		List<DummyEntity> findByNamedRangeWithNamedParameter(@Param("lower") String lower, @Param("upper") String upper);

		@Query("SELECT * FROM DUMMY_ENTITY WHERE name = :name")
		Optional<DummyEntity> findByNameAsOptional(@Param("name") String name);

		// DATAJDBC-172
		@Nullable
		@Query("SELECT * FROM DUMMY_ENTITY WHERE name = :name")
		DummyEntity findByNameAsEntity(@Param("name") String name);

		// DATAJDBC-172
		@Query("SELECT * FROM DUMMY_ENTITY")
		Stream<DummyEntity> findAllWithReturnTypeIsStream();

		// DATAJDBC-175
		@Query("SELECT count(*) FROM DUMMY_ENTITY WHERE name like '%' || :name || '%'")
		int countByNameContaining(@Param("name") String name);

		// DATAJDBC-175
		@Query("SELECT count(*) FROM DUMMY_ENTITY WHERE name like '%' || :name || '%'")
		boolean existsByNameContaining(@Param("name") String name);

		// DATAJDBC-175
		@Query("VALUES (current_timestamp)")
		Date nowWithDate();

		// DATAJDBC-175
		@Query("VALUES (current_timestamp),(current_timestamp)")
		List<LocalDateTime> nowWithLocalDateTimeList();

		// DATAJDBC-182
		@Modifying
		@Query("UPDATE DUMMY_ENTITY SET name = :name WHERE id = :id")
		int updateName(@Param("id") Long id, @Param("name") String name);

		// DATAJDBC-182
		@Modifying
		@Query("DELETE FROM DUMMY_ENTITY WHERE name = :name")
		boolean deleteByName(@Param("name") String name);

		// DATAJDBC-182
		@Modifying
		@Query("INSERT INTO DUMMY_ENTITY (name) VALUES(:name)")
		void insert(@Param("name") String name);

		// DATAJDBC-252
		@Query("SELECT 'one' one, 'two' two, 3 three FROM (VALUES (0))")
		ImmutableTuple immutableTuple();


		@Value
		class ImmutableTuple {
			String one;
			String two;
			int three;
		}
	}
}
