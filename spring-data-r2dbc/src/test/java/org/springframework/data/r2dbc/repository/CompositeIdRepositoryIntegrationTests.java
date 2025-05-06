/*
 * Copyright 2025 the original author or authors.
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
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for repositories of entities with a composite id.
 *
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
public class CompositeIdRepositoryIntegrationTests {

	@Autowired private WithCompositeIdRepository repository;
	private JdbcTemplate jdbc;

	@Configuration
	@EnableR2dbcRepositories(includeFilters = @ComponentScan.Filter(value = WithCompositeIdRepository.class,
			type = FilterType.ASSIGNABLE_TYPE), considerNestedRepositories = true)
	static class TestConfiguration extends AbstractR2dbcConfiguration {
		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}

	}

	@BeforeEach
	void before() {

		this.jdbc = new JdbcTemplate(createDataSource());

		try {
			this.jdbc.execute("DROP TABLE with_composite_id");
		} catch (DataAccessException e) {}

		this.jdbc.execute("""
				CREATE TABLE with_composite_id (
				    one int,
				    two varchar(255),
				    name varchar(255),
				    primary key (one, two))""");
		this.jdbc.execute("INSERT INTO with_composite_id VALUES (42, 'HBAR','Walter')");
		this.jdbc.execute("INSERT INTO with_composite_id VALUES (23, '2PI','Jesse')");
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected ConnectionFactory createConnectionFactory() {
		return H2TestSupport.createConnectionFactory();
	}

	@Test // GH-574
	void findAllById() {
		repository.findById(new CompositeId(42, "HBAR")) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.name).isEqualTo("Walter");
				}).verifyComplete();
	}

	interface WithCompositeIdRepository extends ReactiveCrudRepository<WithCompositeId, CompositeId> {

	}

	@Table("with_composite_id")
	record WithCompositeId(@Id @Embedded.Nullable CompositeId pk, String name) {
	}

	record CompositeId(Integer one, String two) {
	}

}
