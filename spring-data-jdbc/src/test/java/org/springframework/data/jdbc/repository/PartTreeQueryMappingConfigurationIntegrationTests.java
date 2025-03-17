package org.springframework.data.jdbc.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.config.DefaultQueryMappingConfiguration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.PartTreeJdbcQuery;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.RowMapper;

/**
 * Tests for mapping the results of {@link PartTreeJdbcQuery} execution via custom {@link QueryMappingConfiguration}
 *
 * @author Mikhail Polivakha
 */
@IntegrationTest
public class PartTreeQueryMappingConfigurationIntegrationTests {

    @Configuration
    @Import(TestConfiguration.class)
    @EnableJdbcRepositories(
      considerNestedRepositories = true,
      includeFilters = @ComponentScan.Filter(value = CarRepository.class, type = FilterType.ASSIGNABLE_TYPE))
    static class Config {

        @Bean
        QueryMappingConfiguration mappers(@Qualifier("CustomRowMapperBean") CustomRowMapperBean rowMapperBean) {
            return new DefaultQueryMappingConfiguration().registerRowMapper(Car.class, rowMapperBean);
        }

        @Bean(value = "CustomRowMapperBean")
        public CustomRowMapperBean rowMapperBean() {
            return new CustomRowMapperBean();
        }
    }

    @Autowired
    private CarRepository carRepository;

    @Test // DATAJDBC-1006
    void testCustomQueryMappingConfiguration_predefinedPartTreeQuery() {

        // given
        Car saved = carRepository.save(new Car(null, "test-model"));

        // when
        Optional<Car> found = carRepository.findById(saved.getId());

        // then
        Assertions.assertThat(found).isPresent().hasValueSatisfying(car -> Assertions.assertThat(car.getModel()).isEqualTo("STUB"));
    }

    @Test // DATAJDBC-1006
    void testCustomQueryMappingConfiguration_customPartTreeQuery() {

        // given
        Car saved = carRepository.save(new Car(null, "test-model"));

        // when
        Optional<Car> found = carRepository.findOneByModel("test-model");

        // then
        Assertions.assertThat(found).isPresent().hasValueSatisfying(car -> Assertions.assertThat(car.getModel()).isEqualTo("STUB"));
    }

    public static class CustomRowMapperBean implements RowMapper<Car> {

        @Override
        public Car mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Car(rs.getLong("id"), "STUB");
        }
    }

    interface CarRepository extends CrudRepository<Car, Long> {

        Optional<Car> findOneByModel(String model);
    }

    public static class Car {

        @Id
        private Long id;
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

}
