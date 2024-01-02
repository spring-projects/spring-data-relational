/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Base class to test <code>@EnableJdbcRepositories(queryLookupStrategy = ...)</code>
 * 
 * @author Diego Krupitza
 * @since 2.4
 */
@EnabledOnDatabase(DatabaseType.HSQL)
abstract class AbstractJdbcRepositoryLookUpStrategyTests {

	@Autowired protected OnesRepository onesRepository;
	@Autowired NamedParameterJdbcTemplate template;
	@Autowired RelationalMappingContext context;

	void insertTestInstances() {

		AggregateOne firstAggregate = new AggregateOne("Diego");
		AggregateOne secondAggregate = new AggregateOne("Franz");
		AggregateOne thirdAggregate = new AggregateOne("Daniela");

		onesRepository.saveAll(Arrays.asList(firstAggregate, secondAggregate, thirdAggregate));
	}

	void callDeclaredQuery(String name, int expectedSize, String... expectedNames) {

		insertTestInstances();

		List<AggregateOne> likeNameD = onesRepository.findAllByName(name);

		assertThat(likeNameD).hasSize(expectedSize);

		assertThat(likeNameD.stream().map(item -> item.name).collect(Collectors.toList())) //
				.contains(expectedNames);

	}

	protected void callDerivedQuery() {
		insertTestInstances();

		AggregateOne diego = onesRepository.findByName("Diego");
		assertThat(diego).isNotNull();
		assertThat(diego.id).isNotNull();
		assertThat(diego.name).isEqualToIgnoringCase("Diego");
	}

	interface OnesRepository extends CrudRepository<AggregateOne, Long> {

		// if derived is used it is just a basic findByName
		// if declared is used it should be a like check
		@Query("Select * from aggregate_one where NAME like concat('%', :name, '%') ")
		List<AggregateOne> findAllByName(String name);

		AggregateOne findByName(String name);
	}

	static class AggregateOne {

		@Id Long id;
		String name;

		public AggregateOne(String name) {
			this.name = name;
		}
	}
}
