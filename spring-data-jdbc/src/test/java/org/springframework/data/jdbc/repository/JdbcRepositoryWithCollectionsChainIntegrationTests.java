package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests with collections chain.
 *
 * @author Yunyoung LEE
 */
@ContextConfiguration
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryWithCollectionsChainIntegrationTests {

    @Autowired NamedParameterJdbcTemplate template;
    @Autowired DummyEntityRepository repository;

    private static DummyEntity createDummyEntity() {

        DummyEntity entity = new DummyEntity();
        entity.setName("Entity Name");
        return entity;
    }

    @Test // DATAJDBC-551
    public void deleteByName() {

        ChildElement element1 = createChildElement("one");
        ChildElement element2 = createChildElement("two");

        DummyEntity entity = createDummyEntity();
        entity.content.add(element1);
        entity.content.add(element2);

        entity = repository.save(entity);

        assertThat(repository.deleteByName("Entity Name")).isEqualTo(1);

        assertThat(repository.findById(entity.id)).isEmpty();

        Long count = template.queryForObject("select count(1) from grand_child_element", new HashMap<>(), Long.class);
        assertThat(count).isEqualTo(0);
    }

    private ChildElement createChildElement(String name) {

        ChildElement element = new ChildElement();
        element.name = name;
        element.content.add(createGrandChildElement(name + "1"));
        element.content.add(createGrandChildElement(name + "2"));
        return element;
    }

    private GrandChildElement createGrandChildElement(String content) {

        GrandChildElement element = new GrandChildElement();
        element.content = content;
        return element;
    }

    interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
        long deleteByName(String name);
    }

    @Configuration
    @Import(TestConfiguration.class)
    static class Config {

        @Autowired JdbcRepositoryFactory factory;

        @Bean
        Class<?> testClass() {
            return JdbcRepositoryWithCollectionsChainIntegrationTests.class;
        }

        @Bean
        DummyEntityRepository dummyEntityRepository() {
            return factory.getRepository(DummyEntityRepository.class);
        }
    }

    @Data
    static class DummyEntity {

        String name;
        Set<ChildElement> content = new HashSet<>();
        @Id private Long id;

    }

    @RequiredArgsConstructor
    static class ChildElement {

        String name;
        Set<GrandChildElement> content = new HashSet<>();
        @Id private Long id;
    }

    @RequiredArgsConstructor
    static class GrandChildElement {

        String content;
        @Id private Long id;
    }

}
