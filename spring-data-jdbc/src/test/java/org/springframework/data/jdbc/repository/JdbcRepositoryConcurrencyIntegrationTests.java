/*
 * Copyright 2020 the original author or authors.
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
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.DatabaseProfileValueSource;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.annotation.ProfileValueSourceConfiguration;
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

/** Tests that highly concurrent update operations of an entity don't cause deadlocks.
 *
 * @author Myeonghyeon Lee
 * @author Jens Schauder
 */
@ContextConfiguration
@ProfileValueSourceConfiguration(DatabaseProfileValueSource.class)
@IfProfileValue(name = "current.database.is.not.mysql", value = "false")
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

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired PlatformTransactionManager transactionManager;

	@Test // DATAJDBC-488
	public void updateConcurrencyWithEmptyReferences() throws Exception {

		DummyEntity entity = createDummyEntity();
		entity = repository.save(entity);

		assertThat(entity.getId()).isNotNull();

		List<DummyEntity> concurrencyEntities = createEntityStates(entity);

		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);

		List<Exception> exceptions = new CopyOnWriteArrayList<>();
		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size()); // latch for all threads to wait on.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size()); // latch for main thread to wait on until all threads are done.

		concurrencyEntities.stream() //
				.map(e -> new Thread(() -> {

					try {

						startLatch.countDown();
						startLatch.await();

						transactionTemplate.execute(status -> repository.save(e));
					} catch (Exception ex) {
						exceptions.add(ex);
					} finally {
						doneLatch.countDown();
					}
				})) //
				.forEach(Thread::start);

		doneLatch.await();

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);
		assertThat(reloaded.content).hasSize(2);
		assertThat(exceptions).isEmpty();
	}

	@Test // DATAJDBC-493
	public void updateConcurrencyWithDelete() throws Exception {

		DummyEntity entity = createDummyEntity();
		entity = repository.save(entity);

		Long targetId = entity.getId();
		assertThat(targetId).isNotNull();

		List<DummyEntity> concurrencyEntities = createEntityStates(entity);

		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);

		List<Exception> exceptions = new CopyOnWriteArrayList<>();
		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for all threads to wait on.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for main thread to wait on until all threads are done.

		// update
		concurrencyEntities.stream() //
			.map(e -> new Thread(() -> {

				try {

					startLatch.countDown();
					startLatch.await();

					transactionTemplate.execute(status -> repository.save(e));
				} catch (Exception ex) {
					// When the delete execution is complete, the Update execution throws an IncorrectUpdateSemanticsDataAccessException.
					if (ex.getCause() instanceof IncorrectUpdateSemanticsDataAccessException) {
						return;
					}

					exceptions.add(ex);
				} finally {
					doneLatch.countDown();
				}
			})) //
			.forEach(Thread::start);

		// delete
		new Thread(() -> {
			try {

				startLatch.countDown();
				startLatch.await();

				transactionTemplate.execute(status -> {
					repository.deleteById(targetId);
					return null;
				});
			} catch (Exception ex) {
				exceptions.add(ex);
			} finally {
				doneLatch.countDown();
			}
		}).start();

		doneLatch.await();

		assertThat(exceptions).isEmpty();
		assertThat(repository.findById(entity.id)).isEmpty();
	}

	@Test // DATAJDBC-493
	public void updateConcurrencyWithDeleteAll() throws Exception {

		DummyEntity entity = createDummyEntity();
		entity = repository.save(entity);

		List<DummyEntity> concurrencyEntities = createEntityStates(entity);

		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);

		List<Exception> exceptions = new CopyOnWriteArrayList<>();
		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for all threads to wait on.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for main thread to wait on until all threads are done.

		// update
		concurrencyEntities.stream() //
			.map(e -> new Thread(() -> {

				try {

					startLatch.countDown();
					startLatch.await();

					transactionTemplate.execute(status -> repository.save(e));
				} catch (Exception ex) {
					// When the delete execution is complete, the Update execution throws an IncorrectUpdateSemanticsDataAccessException.
					if (ex.getCause() instanceof IncorrectUpdateSemanticsDataAccessException) {
						return;
					}

					exceptions.add(ex);
				} finally {
					doneLatch.countDown();
				}
			})) //
			.forEach(Thread::start);

		// delete
		new Thread(() -> {
			try {

				startLatch.countDown();
				startLatch.await();

				transactionTemplate.execute(status -> {
					repository.deleteAll();
					return null;
				});
			} catch (Exception ex) {
				exceptions.add(ex);
			} finally {
				doneLatch.countDown();
			}
		}).start();

		doneLatch.await();

		assertThat(exceptions).isEmpty();
		assertThat(repository.count()).isEqualTo(0);
	}

	private List<DummyEntity> createEntityStates(DummyEntity entity) {

		List<DummyEntity> concurrencyEntities = new ArrayList<>();
		Element element1 = new Element(null, 1L);
		Element element2 = new Element(null, 2L);

		for (int i = 0; i < 100; i++) {

			List<Element> newContent = Arrays.asList(element1.withContent(element1.content + i + 2),
					element2.withContent(element2.content + i + 2));

			concurrencyEntities.add(entity.withName(entity.getName() + i).withContent(newContent));
		}
		return concurrencyEntities;
	}

	private static DummyEntity createDummyEntity() {
		return new DummyEntity(null, "Entity Name", new ArrayList<>());
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Getter
	@AllArgsConstructor
	static class DummyEntity {

		@Id private Long id;
		@With String name;
		@With final List<Element> content;

	}

	@AllArgsConstructor
	static class Element {

		@Id private Long id;
		@With final Long content;
	}
}
