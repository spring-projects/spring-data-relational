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
package org.springframework.data.r2dbc.repository.support;

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
import org.springframework.data.jdbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link SimpleR2dbcRepository}.
 *
 * @author Mark Paluch
 */
public class SimpleR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	private static RelationalMappingContext mappingContext = new RelationalMappingContext();

	private ConnectionFactory connectionFactory;
	private DatabaseClient databaseClient;
	private SimpleR2dbcRepository<LegoSet, Integer> repository;
	private JdbcTemplate jdbc;

	@Before
	public void before() {

		Hooks.onOperatorDebug();

		this.connectionFactory = createConnectionFactory();
		this.databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(mappingContext, new EntityInstantiators())).build();

		RelationalEntityInformation<LegoSet, Integer> entityInformation = new MappingRelationalEntityInformation<>(
				(RelationalPersistentEntity<LegoSet>) mappingContext.getRequiredPersistentEntity(LegoSet.class));

		this.repository = new SimpleR2dbcRepository<>(entityInformation, databaseClient,
				new MappingR2dbcConverter(mappingContext));

		this.jdbc = createJdbcTemplate(createDataSource());

		String tableToCreate = "CREATE TABLE IF NOT EXISTS repo_legoset (\n" + "    id          SERIAL PRIMARY KEY,\n"
				+ "    name        varchar(255) NOT NULL,\n" + "    manual      integer NULL\n" + ");";

		this.jdbc.execute("DROP TABLE IF EXISTS repo_legoset");
		this.jdbc.execute(tableToCreate);
	}

	@Test
	public void shouldSaveNewObject() {

		LegoSet legoSet = new LegoSet(null, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isNotNull();
				}).verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM repo_legoset");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsEntry("manual", 12).containsKey("id");
	}

	@Test
	public void shouldUpdateObject() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		LegoSet legoSet = new LegoSet(42055, "SCHAUFELRADBAGGER", 12);
		legoSet.setManual(14);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM repo_legoset");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsEntry("manual", 14).containsKey("id");
	}

	@Test
	public void shouldSaveObjectsUsingIterable() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 2L);
	}

	@Test
	public void shouldSaveObjectsUsingPublisher() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Flux.just(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 2L);
	}

	@Test
	public void shouldFindById() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.findById(42055) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test
	public void shouldExistsById() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.existsById(42055) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(42) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	public void shouldExistsByIdPublisher() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.existsById(Mono.just(42055)) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(Mono.just(42)) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	public void shouldFindByAll() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		repository.findAll() //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldFindAllByIdUsingIterable() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		repository.findAllById(Arrays.asList(42055, 42064)) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldFindAllByIdUsingPublisher() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		repository.findAllById(Flux.just(42055, 42064)) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	public void shouldCount() {

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test
	public void shouldDeleteById() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.deleteById(42055) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 0L);
	}

	@Test
	public void shouldDeleteByIdPublisher() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.deleteById(Mono.just(42055)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 0L);
	}

	@Test
	public void shouldDelete() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		LegoSet legoSet = new LegoSet(42055, "SCHAUFELRADBAGGER", 12);

		repository.delete(legoSet) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 0L);
	}

	@Test
	public void shouldDeleteAllUsingIterable() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		LegoSet legoSet = new LegoSet(42055, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Collections.singletonList(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 0L);
	}

	@Test
	public void shouldDeleteAllUsingPublisher() {

		jdbc.execute("INSERT INTO repo_legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		LegoSet legoSet = new LegoSet(42055, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Mono.just(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT COUNT(*) FROM repo_legoset");
		assertThat(map).containsEntry("count", 0L);
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
}
