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

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.name = "Entity Name";
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
			return JdbcRepositoryWithCollectionsChainHsqlIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
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

}
