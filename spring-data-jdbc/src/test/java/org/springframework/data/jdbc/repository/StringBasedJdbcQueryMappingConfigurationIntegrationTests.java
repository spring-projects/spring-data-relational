/*
 * Copyright 2017-2023 the original author or authors.
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

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.DefaultQueryMappingConfiguration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Very simple use cases for creation and usage of {@link ResultSetExtractor}s in JdbcRepository.
 *
 * @author Evgeni Dimitrov
 * @author Hebert Coelho
 */
@IntegrationTest
public class StringBasedJdbcQueryMappingConfigurationIntegrationTests {

	private final static String CAR_MODEL = "ResultSetExtractor Car";
	private final static String VALUE_PROCESSED_BY_SERVICE = "Value Processed by Service";

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CarRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

		@Bean
		QueryMappingConfiguration mappers() {
			return new DefaultQueryMappingConfiguration();
		}

		@Bean(value = "CarResultSetExtractorBean")
		public CarResultSetExtractorBean resultSetExtractorBean() {
			return new CarResultSetExtractorBean();
		}

		@Bean
		public CustomerService service() {
			return new CustomerService();
		}

		@Bean(value = "CustomRowMapperBean")
		public CustomRowMapperBean rowMapperBean() {
			return new CustomRowMapperBean();
		}

	}

	public static class CarResultSetExtractorBean implements ResultSetExtractor<List<Car>> {

		@Autowired private CustomerService customerService;

		@Override
		public List<Car> extractData(ResultSet rs) throws SQLException, DataAccessException {
			return List.of(new Car(1L, customerService.process()));
		}

	}

	public static class CustomRowMapperBean implements RowMapper<String> {

		@Autowired private CustomerService customerService;

		@Override
		public String mapRow(ResultSet rs, int rowNum) throws SQLException {
			return customerService.process();
		}
	}

	public static class CustomerService {
		public String process() {
			return VALUE_PROCESSED_BY_SERVICE;
		}
	}

	public static class Car {

		@Id private Long id;
		private String model;

		public Car(Long id, String model) {
			this.id = id;
			this.model = model;
		}

		public Long getId() {
			return this.id;
		}

		public String getModel() {
			return this.model;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setModel(String model) {
			this.model = model;
		}
	}

	static class CarResultSetExtractor implements ResultSetExtractor<List<Car>> {

		@Override
		public List<Car> extractData(ResultSet rs) throws SQLException, DataAccessException {
			return singletonList(new Car(1L, CAR_MODEL));
		}
	}

	public static class RowMapperResultSetExtractor implements ResultSetExtractor<RowMapper> {

		final RowMapper rowMapper;

		public RowMapperResultSetExtractor(RowMapper rowMapper) {
			this.rowMapper = rowMapper;
		}

		@Override
		public RowMapper extractData(ResultSet rs) throws SQLException, DataAccessException {
			return rowMapper;
		}
	}

	interface CarRepository extends CrudRepository<Car, Long> {

		@Query(value = "select * from car", resultSetExtractorClass = CarResultSetExtractor.class)
		List<Car> customFindAll();

		@Query(value = "select * from car", resultSetExtractorRef = "CarResultSetExtractorBean")
		List<Car> findByNameWithResultSetExtractor();

		@Query(value = "select model from car", rowMapperRef = "CustomRowMapperBean")
		List<String> findByNameWithRowMapperBean();

		@Query(value = "select * from car", resultSetExtractorClass = RowMapperResultSetExtractor.class)
		RowMapper customFindAllWithRowMapper();

	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired CarRepository carRepository;

	@Test // DATAJDBC-290
	void customFindAllCarsUsesConfiguredResultSetExtractor() {

		carRepository.save(new Car(null, "Some model"));
		Iterable<Car> cars = carRepository.customFindAll();

		assertThat(cars).hasSize(1);
		assertThat(cars).allMatch(car -> CAR_MODEL.equals(car.getModel()));
	}

	@Test // DATAJDBC-430
	void customFindWithRowMapperBeanSupportingInjection() {

		carRepository.save(new Car(null, "Some model"));
		List<String> names = carRepository.findByNameWithRowMapperBean();

		assertThat(names).hasSize(1);
		assertThat(names).allMatch(name -> VALUE_PROCESSED_BY_SERVICE.equals(name));
	}

	@Test // DATAJDBC-430
	void customFindWithResultSetExtractorBeanSupportingInjection() {

		carRepository.save(new Car(null, "Some model"));
		Iterable<Car> cars = carRepository.findByNameWithResultSetExtractor();

		assertThat(cars).hasSize(1);
		assertThat(cars).allMatch(car -> VALUE_PROCESSED_BY_SERVICE.equals(car.getModel()));
	}

	@Test // DATAJDBC-620
	void defaultRowMapperGetsInjectedIntoCustomResultSetExtractor() {

		RowMapper rowMapper = carRepository.customFindAllWithRowMapper();

		assertThat(rowMapper).isNotNull();
	}
}
