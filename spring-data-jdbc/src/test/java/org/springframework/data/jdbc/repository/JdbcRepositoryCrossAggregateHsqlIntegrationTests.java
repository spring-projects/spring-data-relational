/*
 * Copyright 2017-2024 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 * @author Salim Achouche
 * @author Salim Achouche
 */

@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
public class JdbcRepositoryCrossAggregateHsqlIntegrationTests {

	private static final long TWO_ID = 23L;

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = Ones.class, type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired Ones ones;
	@Autowired RelationalMappingContext context;

	@SuppressWarnings("ConstantConditions")
	@Test // DATAJDBC-221
	public void savesAndRead() {

		AggregateOne one = new AggregateOne();
		one.name = "Aggregate - 1";
		one.two = AggregateReference.to(TWO_ID);

		one = ones.save(one);

		AggregateOne reloaded = ones.findById(one.id).get();
		assertThat(reloaded.two.getId()).isEqualTo(TWO_ID);
	}

	@Test // DATAJDBC-221
	public void savesAndUpdate() {

		AggregateOne one = new AggregateOne();
		one.name = "Aggregate - 1";
		one.two = AggregateReference.to(42L);
		one = ones.save(one);

		one.two = AggregateReference.to(TWO_ID);

		ones.save(one);

		assertThat( //
				JdbcTestUtils.countRowsInTableWhere( //
						template.getJdbcOperations(), //
						"aggregate_one", //
						"two = " + TWO_ID) //
		).isEqualTo(1);
	}

	interface Ones extends CrudRepository<AggregateOne, Long> {}

	static class AggregateOne {

		@Id Long id;
		String name;
		AggregateReference<AggregateTwo, Long> two;
	}

	static class AggregateTwo {

		@Id Long id;
		String name;
	}
}
