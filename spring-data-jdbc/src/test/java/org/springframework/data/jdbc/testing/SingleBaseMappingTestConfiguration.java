package org.springframework.data.jdbc.testing;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

@Configuration
public class SingleBaseMappingTestConfiguration {

    public final static String VALUE_PROCESSED_BY_SERVICE = "Value Processed by Service";

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

    public static class CarResultSetExtractorBean implements ResultSetExtractor<List<Car>> {

        @Autowired
        private CustomerService customerService;

        @Override
        public List<Car> extractData(ResultSet rs) throws SQLException, DataAccessException {
            return Arrays.asList(new Car(1L, customerService.process()));
        }

    }

    public static class CustomRowMapperBean implements RowMapper<String> {

        @Autowired
        private CustomerService customerService;

        public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            return customerService.process();
        }
    }

    public static class CustomerService {
        public String process() {
            return VALUE_PROCESSED_BY_SERVICE;
        }
    }

    @Data
    @AllArgsConstructor
    public static class Car {

        @Id
        private Long id;
        private String model;
    }
}
