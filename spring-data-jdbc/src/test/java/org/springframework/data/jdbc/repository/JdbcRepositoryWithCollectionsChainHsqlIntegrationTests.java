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
 * Integration tests with collections chain.
 *
 * @author Yunyoung LEE
 * @author Nikita Konev
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
class JdbcRepositoryWithCollectionsChainHsqlIntegrationTests {

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired CustomIdDummyEntityRepository customIdRepository;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.name = "Entity Name";
		return entity;
	}

	private static CustomIdDummyEntity createCustomIdDummyEntity() {

		CustomIdDummyEntity entity = new CustomIdDummyEntity();
		entity.name = "Custom ID Entity Name";
		return entity;
	}

	@Test // DATAJDBC-551
	void deleteByName() {

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

	@Test // DATAJDBC-2123
	void deleteByNameWithCustomIdColumn() {

		CustomIdChildElement element1 = createCustomIdChildElement("one");
		CustomIdChildElement element2 = createCustomIdChildElement("two");

		CustomIdDummyEntity entity = createCustomIdDummyEntity();
		entity.content.add(element1);
		entity.content.add(element2);

		entity = customIdRepository.save(entity);

		assertThat(customIdRepository.deleteByName("Custom ID Entity Name")).isEqualTo(1);

		assertThat(customIdRepository.findById(entity.id)).isEmpty();

		Long count = template.queryForObject("select count(1) from custom_id_grand_child_element", new HashMap<>(), Long.class);
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

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
		long deleteByName(String name);
	}

	interface CustomIdDummyEntityRepository extends CrudRepository<CustomIdDummyEntity, Long> {
		long deleteByName(String name);
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryWithCollectionsChainHsqlIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		CustomIdDummyEntityRepository customIdDummyEntityRepository() {
			return factory.getRepository(CustomIdDummyEntityRepository.class);
		}
	}

	static class DummyEntity {

		String name;
		Set<ChildElement> content = new HashSet<>();
		@Id private Long id;

	}

	static class ChildElement {

		String name;
		Set<GrandChildElement> content = new HashSet<>();
		@Id private Long id;
	}

	static class GrandChildElement {

		String content;
		@Id private Long id;
	}

    static class CustomIdDummyEntity {

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
