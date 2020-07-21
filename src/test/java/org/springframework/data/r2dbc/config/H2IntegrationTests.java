/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.config;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for {@link DatabaseClient} and repositories using H2.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class H2IntegrationTests {

	private JdbcTemplate jdbc = new JdbcTemplate(H2TestSupport.createDataSource());

	@Autowired DatabaseClient databaseClient;
	@Autowired H2Repository repository;

	@Before
	public void before() {

		try {
			jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}
		jdbc.execute(H2TestSupport.CREATE_TABLE_LEGOSET);
	}

	@Test // gh-109
	public void shouldSelectCountWithDatabaseClient() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		databaseClient.sql("SELECT COUNT(*) FROM legoset") //
				.map(it -> it.get(0, Long.class)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // gh-109
	public void shouldSelectCountWithRepository() {

		jdbc.execute("INSERT INTO legoset (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		repository.selectCount() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = H2Repository.class),
			basePackageClasses = H2Repository.class)
	static class H2Configuration extends AbstractR2dbcConfiguration {

		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}
	}

	interface H2Repository extends ReactiveCrudRepository<LegoSet, Integer> {

		@Query("SELECT COUNT(*) FROM legoset")
		Mono<Long> selectCount();
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
}
