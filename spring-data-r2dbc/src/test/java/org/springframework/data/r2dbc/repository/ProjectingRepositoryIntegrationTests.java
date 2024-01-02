/*
 * Copyright 2019-2024 the original author or authors.
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

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
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
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests projections.
 *
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
public class ProjectingRepositoryIntegrationTests {

	@Autowired
	private ImmutableObjectRepository repository;
	private JdbcTemplate jdbc;

	@Configuration
	@EnableR2dbcRepositories(
			includeFilters = @ComponentScan.Filter(value = ImmutableObjectRepository.class, type = FilterType.ASSIGNABLE_TYPE),
			considerNestedRepositories = true)
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
			this.jdbc.execute("DROP TABLE immutable_non_null");
		}
		catch (DataAccessException e) {
		}

		this.jdbc.execute("CREATE TABLE immutable_non_null (id serial PRIMARY KEY, name varchar(255), email varchar(255))");
		this.jdbc.execute("INSERT INTO immutable_non_null VALUES (42, 'Walter', 'heisenberg@the-white-family.com')");
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

	@Test
		// GH-1687
	void shouldApplyProjectionDirectly() {

		repository.findProjectionByEmail("heisenberg@the-white-family.com") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("Walter");
				}).verifyComplete();
	}

	@Test
		// GH-1687
	void shouldApplyEntityQueryProjectionDirectly() {

		repository.findAllByEmail("heisenberg@the-white-family.com") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("Walter");
					assertThat(actual).isInstanceOf(ImmutableNonNullEntity.class);
				}).verifyComplete();
	}

	interface ImmutableObjectRepository extends ReactiveCrudRepository<ImmutableNonNullEntity, Integer> {

		Flux<ProjectionOnNonNull> findProjectionByEmail(String email);

		Flux<Person> findAllByEmail(String email);

	}

	@Table("immutable_non_null")
	static class ImmutableNonNullEntity implements Person {

		final @Nullable
		@Id Integer id;
		final String name;
		final String email;

		ImmutableNonNullEntity(@Nullable Integer id, String name, String email) {

			Assert.notNull(name, "Name must not be null");
			Assert.notNull(email, "Email must not be null");

			this.id = id;
			this.name = name;
			this.email = email;
		}

		@Override
		public String getName() {
			return name;
		}
	}

	interface Person {

		String getName();

	}

	interface ProjectionOnNonNull {

		String getName();

	}

}
