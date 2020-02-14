package org.springframework.data.jdbc.repository;

import junit.framework.AssertionFailedError;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.With;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Myeonghyeon Lee
 */
@ContextConfiguration
@ActiveProfiles("mysql")
public class JdbcRepositoryConcurrencyIntegrationTests {
	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryConcurrencyIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}
	}

	@ClassRule
	public static final SpringClassRule classRule = new SpringClassRule();
	@Rule
	public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired
	NamedParameterJdbcTemplate template;
	@Autowired
	DummyEntityRepository repository;
	@Autowired
	PlatformTransactionManager transactionManager;

	@Test	// DATAJDBC-488
	public void updateConcurrencyWithEmptyReferences() throws Exception {
		DummyEntity entity = createDummyEntity();
		entity = repository.save(entity);

		assertThat(entity.getId()).isNotNull();

		List<DummyEntity> concurrencyEntities = new ArrayList<>();
		Element element1 = new Element(null, 1L);
		Element element2 = new Element(null, 2L);

		for (int i = 0; i < 100; i++) {
			List<Element> newContent = Arrays.asList(
				element1.withContent(element1.content + i + 2),
				element2.withContent(element2.content + i + 2)
			);

			concurrencyEntities.add(entity
				.withName(entity.getName() + i)
				.withContent(newContent));
		}

		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);

		List<Exception> exceptions = new CopyOnWriteArrayList<>();
		CountDownLatch countDownLatch = new CountDownLatch(concurrencyEntities.size());
		concurrencyEntities.stream()
			.map(e -> new Thread(() -> {
				countDownLatch.countDown();
				try {
					transactionTemplate.execute(status -> repository.save(e));
				} catch (Exception ex) {
					exceptions.add(ex);
				}
			}))
			.forEach(Thread::start);

		countDownLatch.await();

		Thread.sleep(1000);
		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);
		assertThat(reloaded.content).hasSize(2);
		assertThat(exceptions).isEmpty();
	}

	private static DummyEntity createDummyEntity() {
		return new DummyEntity(null, "Entity Name", new ArrayList<>());
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
	}

	@Getter
	@AllArgsConstructor
	static class DummyEntity {

		@Id
		private Long id;
		@With
		String name;
		@With
		final List<Element> content;

	}

	@AllArgsConstructor
	static class Element {

		@Id private Long id;
		@With final Long content;
	}
}
