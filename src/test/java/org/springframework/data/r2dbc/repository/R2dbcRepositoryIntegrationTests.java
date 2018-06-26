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

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.TransactionalDatabaseClient;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 */
public class R2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	private static RelationalMappingContext mappingContext = new RelationalMappingContext();

	private ConnectionFactory connectionFactory;
	private DatabaseClient databaseClient;
	private LegoSetRepository repository;
	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

		this.connectionFactory = createConnectionFactory();
		this.databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(mappingContext, new EntityInstantiators())).build();

		this.jdbc = createJdbcTemplate(createDataSource());

		String tableToCreate = "CREATE TABLE IF NOT EXISTS repo_legoset (\n" + "    id          SERIAL PRIMARY KEY,\n"
				+ "    name        varchar(255) NOT NULL,\n" + "    manual      integer NULL\n" + ");";

		this.jdbc.execute("DROP TABLE IF EXISTS repo_legoset");
		this.jdbc.execute(tableToCreate);

		this.repository = new R2dbcRepositoryFactory(databaseClient, mappingContext).getRepository(LegoSetRepository.class);
	}

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

		TransactionalDatabaseClient client = TransactionalDatabaseClient.builder().connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(mappingContext, new EntityInstantiators())).build();

		LegoSetRepository transactionalRepository = new R2dbcRepositoryFactory(client, mappingContext)
				.getRepository(LegoSetRepository.class);

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		Flux<Map<String, Object>> transactional = client.inTransaction(db -> {

			return transactionalRepository.save(legoSet1) //
					.map(it -> jdbc.queryForMap("SELECT count(*) FROM repo_legoset"));
		});

		Mono<Map<String, Object>> nonTransactional = transactionalRepository.save(legoSet2) //
				.map(it -> jdbc.queryForMap("SELECT count(*) FROM repo_legoset"));

		transactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 0L)).verifyComplete();
		nonTransactional.as(StepVerifier::create).expectNext(Collections.singletonMap("count", 2L)).verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) FROM repo_legoset");
		assertThat(count).containsEntry("count", 2L);
	}

	interface LegoSetRepository extends ReactiveCrudRepository<LegoSet, Integer> {

		@Query("SELECT * FROM repo_legoset WHERE name like $1")
		Flux<LegoSet> findByNameContains(String name);

		@Query("SELECT * FROM repo_legoset")
		Flux<Named> findAsProjection();

		@Query("SELECT * FROM repo_legoset WHERE manual = $1")
		Mono<LegoSet> findByManual(int manual);
	}

	@Data
	@Table("repo_legoset")
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
