/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.repository;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.Database;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.TransactionalDatabaseClient;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Abstract base class for integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 */
public abstract class AbstractR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	private static RelationalMappingContext mappingContext = new RelationalMappingContext();

	@Autowired private LegoSetRepository repository;
	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

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
	public void shouldInsertNewItems() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test
	public void shouldFindItemsByManual() {

		shouldInsertNewItems();

		repository.findByManual(13) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
				}) //
				.verifyComplete();
	}

	@Test
	public void shouldFindItemsByNameLike() {

		shouldInsertNewItems();

		repository.findByNameContains("%F%") //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldFindApplyingProjection() {

		shouldInsertNewItems();

		repository.findAsProjection() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldInsertItemsTransactional() {

		Database database = Database.findDatabase(createConnectionFactory()).get();
		DefaultReactiveDataAccessStrategy dataAccessStrategy = new DefaultReactiveDataAccessStrategy(
				database.defaultDialect(), new MappingR2dbcConverter(mappingContext));
		TransactionalDatabaseClient client = TransactionalDatabaseClient.builder()
				.connectionFactory(createConnectionFactory()).dataAccessStrategy(dataAccessStrategy).build();

		LegoSetRepository transactionalRepository = new R2dbcRepositoryFactory(client, dataAccessStrategy)
				.getRepository(getRepositoryInterfaceType());

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		Flux<Map<String, Object>> transactional = client.inTransaction(db -> {

			return transactionalRepository.save(legoSet1) //
					.map(it -> jdbc.queryForMap("SELECT count(*) FROM legoset"));
		});

		Mono<Map<String, Object>> nonTransactional = transactionalRepository.save(legoSet2) //
				.map(it -> jdbc.queryForMap("SELECT count(*) FROM legoset"));

		transactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 0L)).verifyComplete();
		nonTransactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 2L)).verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) FROM legoset");
		assertThat(count).containsEntry("count", 2L);
	}

	@NoRepositoryBean
	interface LegoSetRepository extends ReactiveCrudRepository<LegoSet, Integer> {

		Flux<LegoSet> findByNameContains(String name);

		Flux<Named> findAsProjection();

		Mono<LegoSet> findByManual(int manual);
	}

	@Data
	@Table("legoset")
	@AllArgsConstructor
	@NoArgsConstructor
	static class LegoSet {
		@Id Integer id;
		String name;
		Integer manual;
	}

	interface Named {
		String getName();
	}
}
