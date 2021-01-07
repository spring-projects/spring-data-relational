/*
 * Copyright 2019-2021 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.MySqlTestSupport;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory} against MySQL.
 *
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class MySqlR2dbcRepositoryIntegrationTests extends AbstractR2dbcRepositoryIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = MySqlTestSupport.database();

	@Autowired DateTestsRepository dateTestsRepository;

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = { @Filter(classes = MySqlLegoSetRepository.class, type = FilterType.ASSIGNABLE_TYPE),
					@Filter(classes = DateTestsRepository.class, type = FilterType.ASSIGNABLE_TYPE) })
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return MySqlTestSupport.createConnectionFactory(database);
		}
	}

	@Override
	protected DataSource createDataSource() {
		return MySqlTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return MySqlTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return MySqlTestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}

	@Override
	protected Class<? extends LegoSetRepository> getRepositoryInterfaceType() {
		return MySqlLegoSetRepository.class;
	}

	@Test
	public void shouldUserJsr310Types() {

		JdbcTemplate jdbcTemplate = createJdbcTemplate(createDataSource());

		try {
			jdbcTemplate.execute("DROP TABLE date_tests");
		} catch (DataAccessException e) {}

		jdbcTemplate.execute("CREATE TABLE date_tests (id int, created_timestamp TIMESTAMP, created_date datetime);");

		dateTestsRepository.save(new DateTests(null, LocalDateTime.now(), LocalDateTime.now())).as(StepVerifier::create)
				.expectNextCount(1).verifyComplete();
	}

	@Data
	@AllArgsConstructor
	static class DateTests {
		@Id Integer id;
		LocalDateTime createdTimestamp;
		LocalDateTime createdDate;
	}

	interface MySqlLegoSetRepository extends LegoSetRepository {

		@Override
		@Query("SELECT name FROM legoset")
		Flux<Named> findAsProjection();

		@Override
		@Query("SELECT * FROM legoset WHERE manual = :manual")
		Mono<LegoSet> findByManual(int manual);

		@Override
		@Query("SELECT id FROM legoset")
		Flux<Integer> findAllIds();
	}

	interface DateTestsRepository extends ReactiveCrudRepository<DateTests, Integer> {

	}
}
