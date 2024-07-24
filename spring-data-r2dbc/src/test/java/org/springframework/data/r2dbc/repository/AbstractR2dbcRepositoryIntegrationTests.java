/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Abstract base class for integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 * @author Manousos Mathioudakis
 * @author Diego Krupitza
 */
public abstract class AbstractR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	static {
		Hooks.onOperatorDebug();
	}

	@Autowired private LegoSetRepository repository;
	@Autowired private ConnectionFactory connectionFactory;
	protected JdbcTemplate jdbc;

	@BeforeEach
	void before() {

		this.jdbc = createJdbcTemplate(createDataSource());

		try {
			this.jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}

		this.jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Returns the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null, auto-increment</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	protected abstract Class<? extends LegoSetRepository> getRepositoryInterfaceType();

	@Test // GH-2
	void shouldInsertNewItems() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12, true);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13, false);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // GH-2
	void shouldFindItemsByManual() {

		shouldInsertNewItems();

		repository.findByManual(13) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
				}) //
				.verifyComplete();
	}

	@Test // GH-2
	void shouldFindItemsByNameContains() {

		shouldInsertNewItems();

		repository.findByNameContains("F") //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // GH-1654
	void shouldFindItemsByNameContainsWithLimit() {

		shouldInsertNewItems();

		repository.findByNameContains("F", Limit.of(1)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		repository.findByNameContains("F", Limit.unlimited()) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // GH-475, GH-607
	void shouldFindApplyingInterfaceProjection() {

		shouldInsertNewItems();

		repository.findAsProjection() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();

		repository.findBy(WithName.class) //
				.map(WithName::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // GH-475
	void shouldByStringQueryApplyingDtoProjection() {

		shouldInsertNewItems();

		repository.findAsDtoProjection() //
				.map(LegoDto::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // GH-344
	void shouldFindApplyingDistinctProjection() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12, true);
		LegoSet legoSet2 = new LegoSet(null, "SCHAUFELRADBAGGER", 13, false);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		repository.findDistinctBy() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).contains("SCHAUFELRADBAGGER");
				}).verifyComplete();
	}

	@Test // GH-41
	void shouldFindApplyingSimpleTypeProjection() {

		shouldInsertNewItems();

		repository.findAllIds() //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).allMatch(Integer.class::isInstance);
				}).verifyComplete();
	}

	@Test // GH-698
	void shouldBeTrue() {
		shouldInsertNewItems();

		repository.findLegoSetByFlag(true) //
				.map(a -> a.flag) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).contains(true);
				}).verifyComplete();
	}

	@Test
	void shouldDeleteUsingQueryMethod() {

		shouldInsertNewItems();

		repository.deleteAllByManual(12) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) AS count FROM legoset");
		assertThat(getCount(count)).satisfies(numberOf(1));
	}

	@Test // GH-335
	void shouldFindByPageable() {

		Flux<LegoSet> sets = Flux.fromStream(IntStream.range(0, 100).mapToObj(value -> {
			return new LegoSet(null, "Set " + value, value);
		}));

		repository.saveAll(sets) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		repository.findAllByOrderByManual(PageRequest.of(0, 10)) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(10).extracting(LegoSet::getManual).containsSequence(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
				}).verifyComplete();

		repository.findAllByOrderByManual(PageRequest.of(19, 5)) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(5).extracting(LegoSet::getManual).containsSequence(95, 96, 97, 98, 99);
				}).verifyComplete();
	}

	@Test // GH-335
	void shouldFindTop10() {

		Flux<LegoSet> sets = Flux
				.fromStream(IntStream.range(0, 100).mapToObj(value -> new LegoSet(null, "Set " + value, value, true)));

		repository.saveAll(sets) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		repository.findFirst10By() //
				.as(StepVerifier::create) //
				.expectNextCount(10) //
				.verifyComplete();
	}

	@Test // GH-341
	void shouldDeleteAll() {

		shouldInsertNewItems();

		repository.deleteAllBy() //
				.as(StepVerifier::create) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // GH-2
	public void shouldInsertItemsTransactional() {

		R2dbcTransactionManager r2dbcTransactionManager = new R2dbcTransactionManager(connectionFactory);
		TransactionalOperator rxtx = TransactionalOperator.create(r2dbcTransactionManager);

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12, true);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13, false);

		Mono<Map<String, Object>> transactional = repository.save(legoSet1) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count FROM legoset")).as(rxtx::transactional);

		Mono<Map<String, Object>> nonTransactional = repository.save(legoSet2) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count FROM legoset"));

		transactional.as(StepVerifier::create).assertNext(actual -> assertThat(getCount(actual)).satisfies(numberOf(0)))
				.verifyComplete();
		nonTransactional.as(StepVerifier::create).assertNext(actual -> assertThat(getCount(actual)).satisfies(numberOf(2)))
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT count(*) AS count FROM legoset");
		assertThat(getCount(map)).satisfies(numberOf(2));
	}

	@Test // GH-363
	void derivedQueryWithCountProjection() {

		shouldInsertNewItems();

		repository.countByNameContains("SCH") //
				.as(StepVerifier::create) //
				.assertNext(i -> assertThat(i).isEqualTo(2)) //
				.verifyComplete();
	}

	@Test // GH-363
	void derivedQueryWithCount() {

		shouldInsertNewItems();

		repository.countByNameContains("SCH") //
				.as(StepVerifier::create) //
				.assertNext(i -> assertThat(i).isEqualTo(2)) //
				.verifyComplete();
	}

	@Test // GH-468
	void derivedQueryWithExists() {

		shouldInsertNewItems();

		repository.existsByName("ABS") //
				.as(StepVerifier::create) //
				.expectNext(Boolean.FALSE) //
				.verifyComplete();

		repository.existsByName("SCHAUFELRADBAGGER") //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // GH-421
	void shouldDeleteAllAndReturnCount() {

		shouldInsertNewItems();

		repository.deleteAllAndReturnCount() //
				.as(StepVerifier::create) //
				.expectNext(2) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // GH-1041
	void getAllByNameWithWriteLock() {

		shouldInsertNewItems();

		repository.getAllByName("SCHAUFELRADBAGGER") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
				}) //
				.verifyComplete();
	}

	@Test // GH-1041
	void findByNameAndFlagWithReadLock() {

		shouldInsertNewItems();

		repository.findByNameAndFlag("SCHAUFELRADBAGGER", true) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.isFlag()).isTrue();
				}) //
				.verifyComplete();
	}

	private static Object getCount(Map<String, Object> map) {
		return map.getOrDefault("count", map.get("COUNT"));
	}

	private Condition<? super Object> numberOf(int expected) {
		return new Condition<>(it -> {
			return it instanceof Number && ((Number) it).intValue() == expected;
		}, "Number  %d", expected);
	}

	@NoRepositoryBean
	interface LegoSetRepository extends ReactiveCrudRepository<LegoSet, Integer> {

		@Lock(LockMode.PESSIMISTIC_WRITE)
		Flux<LegoSet> getAllByName(String name);

		@Lock(LockMode.PESSIMISTIC_READ)
		Flux<LegoSet> findByNameAndFlag(String name, Boolean flag);

		Flux<LegoSet> findByNameContains(String name);

		Flux<LegoSet> findByNameContains(String name, Limit limit);

		Flux<LegoSet> findFirst10By();

		Flux<LegoSet> findAllByOrderByManual(Pageable pageable);

		Flux<Named> findAsProjection();

		<T> Flux<T> findBy(Class<T> theClass);

		@Query("SELECT name from legoset")
		Flux<LegoDto> findAsDtoProjection();

		Flux<Named> findDistinctBy();

		Mono<LegoSet> findByManual(int manual);

		Flux<Integer> findAllIds();

		Mono<Void> deleteAllBy();

		@Modifying
		@Query("DELETE from legoset where manual = :manual")
		Mono<Void> deleteAllByManual(int manual);

		@Modifying
		@Query("DELETE from legoset")
		Mono<Integer> deleteAllAndReturnCount();

		Mono<Integer> countByNameContains(String namePart);

		Mono<Boolean> existsByName(String name);

		Flux<LegoSet> findLegoSetByFlag(boolean flag);
	}

	public interface Buildable {

		String getName();
	}

	@Table("legoset")
	public static class LegoSet extends Lego implements Buildable {
		String name;
		Integer manual;
		boolean flag;

		@PersistenceCreator
		LegoSet(Integer id, String name, Integer manual) {
			super(id);
			this.name = name;
			this.manual = manual;
		}

		LegoSet(Integer id, String name, Integer manual, boolean flag) {
			this(id, name, manual);
			this.flag = flag;
		}

		public LegoSet() {
		}

		@Override
		public String getName() {
			return this.name;
		}

		public Integer getManual() {
			return this.manual;
		}

		public boolean isFlag() {
			return this.flag;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setManual(Integer manual) {
			this.manual = manual;
		}

		public void setFlag(boolean flag) {
			this.flag = flag;
		}
	}

	static class Lego {
		@Id
		Integer id;

		public Lego(Integer id) {
			this.id = id;
		}

		public Lego() {
		}

		public Integer getId() {
			return this.id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}

	static final class LegoDto {
		private final String name;
		private final String unknown;

		public LegoDto(String name, String unknown) {
			this.name = name;
			this.unknown = unknown;
		}

		public String getName() {
			return this.name;
		}

		public String getUnknown() {
			return this.unknown;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof final LegoDto other))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$unknown = this.getUnknown();
			final Object other$unknown = other.getUnknown();
			return Objects.equals(this$unknown, other$unknown);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $unknown = this.getUnknown();
			result = result * PRIME + ($unknown == null ? 43 : $unknown.hashCode());
			return result;
		}

		public String toString() {
			return "AbstractR2dbcRepositoryIntegrationTests.LegoDto(name=" + this.getName() + ", unknown=" + this.getUnknown() + ")";
		}
	}

	interface Named {
		String getName();
	}

	interface WithName {
		String getName();
	}
}
