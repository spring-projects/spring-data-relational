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
package org.springframework.data.jdbc.repository.aot;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for AOT processing via {@link JdbcRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = JdbcRepositoryContributorIntegrationTests.JdbcRepositoryContributorConfiguration.class)
@IntegrationTest
@EnabledOnDatabase(DatabaseType.H2)
class JdbcRepositoryContributorIntegrationTests {

	@Autowired UserRepository fragment;
	@Autowired JdbcAggregateOperations operations;

	@Configuration
	@EnableJdbcRepositories(jdbcAggregateOperationsRef = "jdbcAggregateOperations", considerNestedRepositories = true,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = String.class) })
	@Import(TestConfiguration.class)
	static class JdbcRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public JdbcRepositoryContributorConfiguration() {
			super(UserRepository.class, JdbcH2Dialect.INSTANCE, JdbcRepositoryContributorConfiguration.class);
		}

		@Bean
		TestClass testClass() {
			return TestClass.of(JdbcRepositoryContributorIntegrationTests.class);
		}

		@Bean
		MyRowMapper myRowMapper() {
			return new MyRowMapper();
		}

		@Bean
		SimpleResultSetExtractor simpleResultSetExtractor() {
			return new SimpleResultSetExtractor();
		}

	}

	@BeforeEach
	void beforeEach() {

		operations.deleteAll(User.class);

		operations.insert(new User("Walter", 52));
		operations.insert(new User("Skyler", 40));
		operations.insert(new User("Flynn", 16));
		operations.insert(new User("Mike", 62));
		operations.insert(new User("Gustavo", 51));
		operations.insert(new User("Hector", 83));
	}

	@Test // GH-2121
	void shouldFindByFirstname() {

		User walter = fragment.findByFirstname("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldFindOptionalByFirstname() {

		assertThat(fragment.findOptionalByFirstname("Walter")).isPresent();
		assertThat(fragment.findOptionalByFirstname("Hank")).isEmpty();
	}

	@Test // GH-2121
	void shouldFindByFirstnameLike() {

		User walter = fragment.findByFirstnameLike("%alter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldFindByFirstnameStartingWith() {

		User walter = fragment.findByFirstnameStartingWith("Wa");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");

		walter = fragment.findByFirstnameStartingWith("Wa%");

		assertThat(walter).isNull(); // % is escaped
	}

	@Test // GH-2121
	void shouldFindByFirstnameEndingWith() {

		User walter = fragment.findByFirstnameEndingWith("lter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");

		walter = fragment.findByFirstnameEndingWith("$lter");

		assertThat(walter).isNull(); // % is escaped
	}

	@Test // GH-2121
	void shouldFindBetween() {

		List<User> users = fragment.findAllByAgeBetween(40, 51);

		assertThat(users).hasSize(2);
	}

	@Test // GH-2121
	void streamByAgeGreaterThan() {
		assertThat(fragment.streamByAgeGreaterThan(20)).hasSize(5);
	}

	@Test // GH-2121
	void shouldReturnSlice() {

		Slice<User> slice = fragment.findSliceByAgeGreaterThan(Pageable.ofSize(4), 10);

		assertThat(slice).hasSize(4);

		assertThat(slice.hasNext()).isTrue();
		slice = fragment.findSliceByAgeGreaterThan(Pageable.ofSize(6), 10);

		assertThat(slice).hasSize(6);
		assertThat(slice.hasNext()).isFalse();
	}

	@Test // GH-2121
	void shouldReturnPage() {

		Page<User> page = fragment.findPageByAgeGreaterThan(PageRequest.of(0, 4, Sort.by("age")), 10);

		assertThat(page).hasSize(4);

		assertThat(page.hasNext()).isTrue();
		page = fragment.findPageByAgeGreaterThan(page.nextPageable(), 10);

		assertThat(page).hasSize(2);
		assertThat(page.hasNext()).isFalse();
	}

	@Test // GH-2121
	void countByAgeLessThan() {

		long count = fragment.countByAgeLessThan(20);

		assertThat(count).isOne();
	}

	@Test // GH-2121
	void countShortByAgeLessThan() {

		short count = fragment.countShortByAgeLessThan(20);

		assertThat(count).isOne();
	}

	@Test // GH-2121
	void existsByAgeLessThan() {

		assertThat(fragment.existsByAgeLessThan(20)).isTrue();
		assertThat(fragment.existsByAgeLessThan(5)).isFalse();
	}

	@Test // GH-2121
	void listWithLimit() {

		List<User> users = fragment.findTop5ByOrderByAge();

		assertThat(users).hasSize(5).extracting(User::getFirstname).containsSequence("Flynn", "Skyler", "Gustavo", "Walter",
				"Mike");
	}

	@Test // GH-2121
	void shouldFindAnnotatedByFirstname() {

		User walter = fragment.findByFirstnameAnnotated("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldFindAnnotatedByFirstnameExpression() {

		User walter = fragment.findByFirstnameExpression("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldFindUsingRowMapper() {

		User walter = fragment.findUsingRowMapper("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Row: 0");
	}

	@Test // GH-2121
	void shouldFindUsingRowMapperRef() {

		User walter = fragment.findUsingRowMapperRef("Walter");

		assertThat(walter).isNotNull();
		assertThat(walter.getFirstname()).isEqualTo("Row: 0");
	}

	@Test // GH-2121
	void shouldFindUsingResultSetExtractor() {

		int result = fragment.findUsingAndResultSetExtractor("Walter");

		assertThat(result).isOne();
	}

	@Test // GH-2121
	void shouldFindUsingResultSetExtractorRef() {

		int result = fragment.findUsingAndResultSetExtractorRef("Walter");

		assertThat(result).isOne();
	}

	@Test // GH-2121
	void shouldProjectOneToDto() {

		UserDto dto = fragment.findOneDtoByFirstname("Walter");

		assertThat(dto).isNotNull();
		assertThat(dto.firstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldProjectListToDto() {

		List<UserDto> dtos = fragment.findDtoByFirstname("Walter");

		assertThat(dtos).hasSize(1).extracting(UserDto::firstname).containsOnly("Walter");
	}

	@Test // GH-2121
	void shouldProjectOneToInterface() {

		UserProjection projection = fragment.findOneInterfaceByFirstname("Walter");

		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo("Walter");
	}

	@Test // GH-2121
	void shouldProjectListToInterface() {

		List<UserProjection> projections = fragment.findInterfaceByFirstname("Walter");

		assertThat(projections).hasSize(1).extracting(UserProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-2121
	void shouldProjectDynamically() {

		List<UserDto> dtos = fragment.findDynamicProjectionByFirstname("Walter", UserDto.class);
		assertThat(dtos).hasSize(1).extracting(UserDto::firstname).containsOnly("Walter");

		List<UserProjection> projections = fragment.findDynamicProjectionByFirstname("Walter", UserProjection.class);
		assertThat(projections).hasSize(1).extracting(UserProjection::getFirstname).containsOnly("Walter");
	}

	@Test // GH-2121
	void shouldDeleteByName() {

		assertThat(fragment.deleteByFirstname("Walter")).isTrue();
		assertThat(fragment.deleteByFirstname("Walter")).isFalse();
	}

	@Test // GH-2121
	void shouldDeleteCountByName() {

		assertThat(fragment.deleteCountByFirstname("Walter")).isOne();
		assertThat(fragment.deleteCountByFirstname("Walter")).isZero();
	}

	@Test // GH-2121
	void shouldDeleteAnnotated() {

		assertThat(fragment.deleteAnnotatedQuery("Walter")).isOne();
		assertThat(fragment.deleteAnnotatedQuery("Walter")).isZero();
	}

	@Test // GH-2121
	void shouldDeleteWithoutResult() {

		fragment.deleteWithoutResult("Walter");

		assertThat(fragment.findByFirstname("Walter")).isNull();
	}

	@Test // GH-2121
	void shouldDeleteAndReturnByName() {

		assertThat(fragment.deleteOneByFirstname("Walter")).isNotNull();
		assertThat(fragment.deleteOneByFirstname("Walter")).isNull();
	}

}
