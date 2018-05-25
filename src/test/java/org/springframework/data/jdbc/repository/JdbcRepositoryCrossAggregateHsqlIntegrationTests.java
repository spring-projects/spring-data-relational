/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.Reference;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Very simple use cases for creation and usage of JdbcRepositories.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryCrossAggregateHsqlIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryCrossAggregateHsqlIntegrationTests.class;
		}
	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired Ones ones;
	@Autowired Twos twos;
	@Autowired JdbcMappingContext context;

	@Test // DATAJDBC-221
	public void savesAnEntity() {

		long TWO_ID = 23L;

		AggregateTwo two = new AggregateTwo();
		two.id = TWO_ID; // we can't reference it without id.

		AggregateOne one = new AggregateOne();
		one.name ="Aggregate - 1";
		one.two =Reference.to(context, two);
		one = ones.save(one);

		assertThat( //
				JdbcTestUtils.countRowsInTableWhere( //
						(JdbcTemplate) template.getJdbcOperations(), //
						"aggregate_one", //
				"two = " + TWO_ID) //
		).isEqualTo(1);
	}

	@SuppressWarnings("ConstantConditions")
	@Test // DATAJDBC-221
	public void savesAndRead() {

		AggregateTwo two = new AggregateTwo();
		two.name="the two";
		twos.save(two);

		AggregateOne one = new AggregateOne();
		one.name="Aggregate - 1";
		one.two = Reference.to(two.id);
		one = ones.save(one);

		AggregateOne reloaded = ones.findById(one.id).get();
		assertThat(reloaded.two.getId()).isEqualTo(two.id);
		assertThat(reloaded.two.get().id).isEqualTo(two.id);

	}


	interface Ones extends CrudRepository<AggregateOne, Long> {}

	interface Twos extends CrudRepository<AggregateTwo, Long> {}

	static class AggregateOne {

		@Id Long id;
		String name;
		Reference<AggregateTwo, Long> two;
	}

	static class AggregateTwo {

		@Id Long id;
		String name;
	}
}
