/*
 * Copyright 2018-present the original author or authors.
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
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.repository.query.ReactiveQueryByExampleExecutor;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Tests for support of embedded entities.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
class H2R2dbcRepositoryEmbeddedIntegrationTests extends R2dbcIntegrationTestSupport {

	static {
		Hooks.onOperatorDebug();
	}

	@Autowired private PersonRepository repository;
	protected JdbcTemplate jdbc;

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(classes = PersonRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}

		@Override
		public R2dbcMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy,
				R2dbcCustomConversions r2dbcCustomConversions, RelationalManagedTypes r2dbcManagedTypes) {

			R2dbcMappingContext context = super.r2dbcMappingContext(namingStrategy, r2dbcCustomConversions,
					r2dbcManagedTypes);
			context.setForceQuote(false);

			return context;
		}

		@Bean
		public H2R2dbcRepositoryIntegrationTests.AfterConvertCallbackRecorder afterConvertCallbackRecorder() {
			return new H2R2dbcRepositoryIntegrationTests.AfterConvertCallbackRecorder();
		}
	}

	@BeforeEach
	void before() {

		this.jdbc = createJdbcTemplate(createDataSource());

		try {
			this.jdbc.execute("DROP TABLE person");
		} catch (DataAccessException e) {}

		this.jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	String getCreateTableStatement() {
		return "create table person(id integer AUTO_INCREMENT PRIMARY KEY, name_first varchar(50), name_last varchar(50))";
	}

	@Test // GH-2096
	void shouldInsertNewItems() {

		Person frodo = new Person(null, new Name("Frodo", "Baggins"));
		Person sam = new Person(null, new Name("Sam", "Gamgee"));

		repository.saveAll(Arrays.asList(frodo, sam)) //
				.as(StepVerifier::create) //
				.expectNextMatches(person -> person.id != null) //
				.expectNextMatches(person -> person.id != null) //
				.verifyComplete();
	}

	@Test // GH-2096
	void shouldReadNewItems() {

		shouldInsertNewItems();

		Set<String> firstNames = Set.of("Frodo", "Sam");

		repository.findAll() //
				.as(StepVerifier::create) //
				.assertNext(p -> firstNames.contains(p.name.first)) //
				.assertNext(p -> firstNames.contains(p.name.first)) //
				.verifyComplete();
	}

	@Test // GH-2096
	void shouldFindUsingQueryByExample() {

		shouldInsertNewItems();

		Person probe = new Person(null, new Name("Frodo", "Baggins"));

		repository.findAll(Example.of(probe)) //
				.as(StepVerifier::create) //
				.assertNext(p -> assertThat(p.name.first).isEqualTo("Frodo")) //
				.verifyComplete();
	}

	interface PersonRepository extends ReactiveCrudRepository<Person, Integer>, ReactiveQueryByExampleExecutor<Person> {}

	record Person(@Id Integer id, @Embedded.Empty(prefix = "name_") Name name) {

	}

	record Name(String first, String last) {

	}

}
