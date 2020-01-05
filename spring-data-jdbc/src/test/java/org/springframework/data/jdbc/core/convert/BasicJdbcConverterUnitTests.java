package org.springframework.data.jdbc.core.convert;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

import java.sql.ResultSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link BasicJdbcConverter}.
 *
 * @author Myeonghyeon Lee
 */
public class BasicJdbcConverterUnitTests {

    JdbcMappingContext context = new JdbcMappingContext();
    BasicJdbcConverter converter = new BasicJdbcConverter(context, mock(RelationResolver.class));

    @Test   // DATAJDBC-465
    public void mapRowByConstructorInstantiator() {

        RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);
        ResultSet resultSet = getResultSet();

        Object readValue = converter.mapRow(entity, resultSet, 0);

        Assertions.assertThat(readValue).isExactlyInstanceOf(DummyEntity.class);

        DummyEntity dummyEntity = (DummyEntity) readValue;
        Assertions.assertThat(dummyEntity.id).isEqualTo(1L);
        Assertions.assertThat(dummyEntity.name).isEqualTo("dummy");
    }

    @Test   // DATAJDBC-465
    public void mapRowByConstructorInstantiatorWithTransientProperty() {

        RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntityWithTransient.class);
        ResultSet resultSet = getResultSet();

        Object readValue = converter.mapRow(entity, resultSet, 0);

        Assertions.assertThat(readValue).isExactlyInstanceOf(DummyEntityWithTransient.class);

        DummyEntityWithTransient dummyEntity = (DummyEntityWithTransient) readValue;
        Assertions.assertThat(dummyEntity.id).isEqualTo(1L);
        Assertions.assertThat(dummyEntity.name).isEqualTo("dummy");
        Assertions.assertThat(dummyEntity.temp).isNull();
    }

    @SneakyThrows
    private ResultSet getResultSet() {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject("id")).thenReturn(1L);
        when(resultSet.getObject("name")).thenReturn("dummy");
        when(resultSet.getObject("temp")).thenThrow(new RuntimeException("temp does not exist"));
        return resultSet;
    }

    @AllArgsConstructor
    private static class DummyEntity {

        @Id
        Long id;
        String name;
    }

    @AllArgsConstructor
    private static class DummyEntityWithTransient {

        @Id
        Long id;
        String name;
        @Transient String temp;
    }
}
