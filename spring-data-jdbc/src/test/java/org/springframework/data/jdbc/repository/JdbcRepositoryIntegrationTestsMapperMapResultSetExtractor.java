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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.ConfigurableMapperMap;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Very simple use cases for creation and usage of {@link ResultSetExtractor}s in JdbcRepository.
 *
 * @author Evgeni Dimitrov
 */
@ContextConfiguration
@Transactional
@Ignore
public class JdbcRepositoryIntegrationTestsMapperMapResultSetExtractor {
	private static String CAR_MODEL = "ResultSetExtracotr Car";
	@Configuration
	@Import(TestConfiguration.class)
//	@EnableJdbcRepositories(considerNestedRepositories = true,
//		includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE /*, classes = CarRepository.class*/))
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@PostConstruct
		public void init () {
			factory.setRowMapperMap(mappers());
		}
		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIntegrationTestsMapperMapResultSetExtractor.class;
		}

		@Bean
		CarRepository carEntityRepository() {
			return factory.getRepository(CarRepository.class);
		}
		
		//@Bean
		MapperMap mappers() {
			return new ConfigurableMapperMap()
					.registerResultSetExtractor(Car.class, new CarResultSetExtractor());
		}
	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired CarRepository carRepository;
	
	@Test // DATAJDBC-290
	public void findAllCarsPicksResultSetExtractorFromMapperMap() {
		carRepository.save(new Car(null, "Some model"));
		Iterable<Car> cars = carRepository.findAll();
		assertThat(cars).hasSize(1);
		assertThat(cars).allMatch(car -> CAR_MODEL.equals(car.getModel()));
	}
	
	interface CarRepository extends CrudRepository<Car, Long> {
		@Query("")
		public List<Car> test();
	}
	
	@Data
	@AllArgsConstructor
	static class Car {
		@Id 
		private Long id;
		private String model;
	}
	
	static class CarResultSetExtractor implements ResultSetExtractor<Car> {

		@Override
		public Car extractData(ResultSet rs) throws SQLException, DataAccessException {
			return new Car(1L, CAR_MODEL);
		}
		
	}
}
