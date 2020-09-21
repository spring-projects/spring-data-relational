/*
 * Copyright 2018-2020 the original author or authors.
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Abstract base class for integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 */
public abstract class AbstractR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

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
	 * Returns the the CREATE TABLE statement for table {@code legoset} with the following three columns:
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

	@Test
	void shouldInsertNewItems() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test
	void shouldFindItemsByManual() {

		shouldInsertNewItems();

		repository.findByManual(13) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
				}) //
				.verifyComplete();
	}

	@Test
	void shouldFindItemsByNameLike() {

		shouldInsertNewItems();

		repository.findByNameContains("F") //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	void shouldFindApplyingProjection() {

		shouldInsertNewItems();

		repository.findAsProjection() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // gh-344
	void shouldFindApplyingDistinctProjection() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "SCHAUFELRADBAGGER", 13);

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

	@Test // gh-41
	void shouldFindApplyingSimpleTypeProjection() {

		shouldInsertNewItems();

		repository.findAllIds() //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).allMatch(Integer.class::isInstance);
				}).verifyComplete();
	}

	@Test
	void shouldDeleteUsingQueryMethod() {

		shouldInsertNewItems();

		repository.deleteAllByManual(12) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) AS count FROM legoset");
		assertThat(count).hasEntrySatisfying("count", numberOf(1));
	}

	@Test // gh-335
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

	@Test // gh-335
	void shouldFindTop10() {

		Flux<LegoSet> sets = Flux.fromStream(IntStream.range(0, 100).mapToObj(value -> {
			return new LegoSet(null, "Set " + value, value);
		}));

		repository.saveAll(sets) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		repository.findFirst10By() //
				.as(StepVerifier::create) //
				.expectNextCount(10) //
				.verifyComplete();
	}

	@Test // gh-341
	void shouldDeleteAll() {

		shouldInsertNewItems();

		repository.deleteAllBy() //
				.as(StepVerifier::create) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test
	public void shouldInsertItemsTransactional() {

		R2dbcTransactionManager r2dbcTransactionManager = new R2dbcTransactionManager(connectionFactory);
		TransactionalOperator rxtx = TransactionalOperator.create(r2dbcTransactionManager);

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		Mono<Map<String, Object>> transactional = repository.save(legoSet1) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count FROM legoset")).as(rxtx::transactional);

		Mono<Map<String, Object>> nonTransactional = repository.save(legoSet2) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count FROM legoset"));

		transactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 0L)).verifyComplete();
		nonTransactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 2L)).verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) AS count FROM legoset");
		assertThat(count).hasEntrySatisfying("count", numberOf(2));
	}

	@Test // gh-363
	void derivedQueryWithCountProjection() {

		shouldInsertNewItems();

		repository.countByNameContains("SCH") //
				.as(StepVerifier::create) //
				.assertNext(i -> assertThat(i).isEqualTo(2)) //
				.verifyComplete();
	}

	@Test // gh-421
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

	private Condition<? super Object> numberOf(int expected) {
		return new Condition<>(it -> {
			return it instanceof Number && ((Number) it).intValue() == expected;
		}, "Number  %d", expected);
	}

	@NoRepositoryBean
	interface LegoSetRepository extends ReactiveCrudRepository<LegoSet, Integer> {

		Flux<LegoSet> findByNameContains(String name);

		Flux<LegoSet> findFirst10By();

		Flux<LegoSet> findAllByOrderByManual(Pageable pageable);

		Flux<Named> findAsProjection();

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
	}

	@Getter
	@Setter
	@Table("legoset")
	@NoArgsConstructor
	public static class LegoSet extends Lego {
		String name;
		Integer manual;

		@PersistenceConstructor
		LegoSet(Integer id, String name, Integer manual) {
			super(id);
			this.name = name;
			this.manual = manual;
		}
	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Getter
	@Setter
	static class Lego {
		@Id Integer id;
	}

	interface Named {
		String getName();
	}
}
