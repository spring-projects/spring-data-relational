package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Integration tests for custom ID column names with @Column annotation in DELETE operations.
 * This tests the fix for DATAJDBC-2123.
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
class JdbcRepositoryWithCollectionsCustomIdIntegrationTests {

    @Autowired NamedParameterJdbcTemplate template;
    @Autowired CustomIdEntityRepository repository;

    private static CustomIdEntity createCustomIdEntity() {
        CustomIdEntity entity = new CustomIdEntity();
        entity.name = "Custom ID Entity Name";
        return entity;
    }

    @Test // DATAJDBC-2123
    void deleteByNameWithCustomIdColumn() {

        CustomIdChildElement element1 = createCustomIdChildElement("one");
        CustomIdChildElement element2 = createCustomIdChildElement("two");

        CustomIdEntity entity = createCustomIdEntity();
        entity.content.add(element1);
        entity.content.add(element2);

        entity = repository.save(entity);

        assertThat(repository.deleteByName("Custom ID Entity Name")).isEqualTo(1);

        assertThat(repository.findById(entity.id)).isEmpty();

        Long count = template.queryForObject("select count(1) from custom_id_grand_child_element", new HashMap<>(), Long.class);
        assertThat(count).isEqualTo(0);
    }

    private CustomIdChildElement createCustomIdChildElement(String name) {

        CustomIdChildElement element = new CustomIdChildElement();
        element.name = name;
        element.content.add(createCustomIdGrandChildElement(name + "1"));
        element.content.add(createCustomIdGrandChildElement(name + "2"));
        return element;
    }

    private CustomIdGrandChildElement createCustomIdGrandChildElement(String content) {

        CustomIdGrandChildElement element = new CustomIdGrandChildElement();
        element.content = content;
        return element;
    }

    interface CustomIdEntityRepository extends CrudRepository<CustomIdEntity, Long> {
        long deleteByName(String name);
    }

    @Configuration
    @Import(TestConfiguration.class)
    static class Config {

        @Autowired JdbcRepositoryFactory factory;

        @Bean
        Class<?> testClass() {
            return JdbcRepositoryWithCollectionsCustomIdIntegrationTests.class;
        }

        @Bean
        CustomIdEntityRepository customIdEntityRepository() {
            return factory.getRepository(CustomIdEntityRepository.class);
        }
    }

    static class CustomIdEntity {

        String name;
        Set<CustomIdChildElement> content = new HashSet<>();
        @Id private Long id;
    }

    static class CustomIdChildElement {

        String name;
        Set<CustomIdGrandChildElement> content = new HashSet<>();
        @Id @Column("CHILD_ID") private Long id;
    }

    static class CustomIdGrandChildElement {

        String content;
        @Id private Long id;
    }
}
