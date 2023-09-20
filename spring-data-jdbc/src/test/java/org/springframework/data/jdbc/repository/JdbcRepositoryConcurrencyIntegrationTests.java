/*
 * Copyright 2020-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import junit.framework.AssertionFailedError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Tests that highly concurrent update operations of an entity don't cause deadlocks.
 *
 * @author Myeonghyeon Lee
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryConcurrencyIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		TestClass testClass() {
			return TestClass.of(JdbcRepositoryConcurrencyIntegrationTests.class);
		}

		@Bean
		DummyEntityRepository dummyEntityRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(DummyEntityRepository.class);
		}
	}

	@Autowired
	NamedParameterJdbcTemplate template;
	@Autowired
	DummyEntityRepository repository;
	@Autowired
	PlatformTransactionManager transactionManager;

	List<DummyEntity> concurrencyEntities;
	DummyEntity entity;

	TransactionTemplate transactionTemplate;
	List<Exception> exceptions;

	@BeforeAll
	public static void beforeClass() {

		Assertions.registerFormatterForType(CopyOnWriteArrayList.class, l -> {

			StringJoiner joiner = new StringJoiner(", ", "List(", ")");
			l.forEach(e -> {

				if (e instanceof Throwable) {
					printThrowable(joiner, (Throwable) e);
				} else {
					joiner.add(e.toString());
				}
			});

			return joiner.toString();
		});
	}

	private static void printThrowable(StringJoiner joiner, Throwable t) {

		joiner.add(t + ExceptionUtils.readStackTrace(t));
		if (t.getCause() != null) {

			joiner.add("\ncaused by:\n");
			printThrowable(joiner, t.getCause());
		}
	}

	@BeforeEach
	public void before() {

		entity = repository.save(createDummyEntity());

		assertThat(entity.getId()).isNotNull();

		concurrencyEntities = createEntityStates(entity);

		transactionTemplate = new TransactionTemplate(this.transactionManager);

		exceptions = new CopyOnWriteArrayList<>();
	}

	@Test // DATAJDBC-488
	public void updateConcurrencyWithEmptyReferences() throws Exception {

		// latch for all threads to wait on.
		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size());
		// latch for main thread to wait on until all threads are done.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size());

		UnaryOperator<DummyEntity> action = e -> repository.save(e);

		concurrencyEntities.forEach(e -> executeInParallel(startLatch, doneLatch, action, e));

		doneLatch.await();

		DummyEntity reloaded = repository.findById(entity.id).orElseThrow(AssertionFailedError::new);
		assertThat(reloaded.content).hasSize(2);
		assertThat(exceptions).isEmpty();
	}

	@Test // DATAJDBC-493
	public void concurrentUpdateAndDelete() throws Exception {

		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for all threads to wait on.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for main thread to wait on
		// until all threads are done.
		UnaryOperator<DummyEntity> updateAction = e -> {
			try {
				return repository.save(e);
			} catch (Exception ex) {
				// When the delete execution is complete, the Update execution throws an
				// IncorrectUpdateSemanticsDataAccessException.
				if (ex.getCause() instanceof IncorrectUpdateSemanticsDataAccessException) {
					return null;
				}
				throw ex;
			}
		};

		UnaryOperator<DummyEntity> deleteAction = e -> {
			repository.deleteById(entity.id);
			return null;
		};

		concurrencyEntities.forEach(e -> executeInParallel(startLatch, doneLatch, updateAction, e));
		executeInParallel(startLatch, doneLatch, deleteAction, entity);

		doneLatch.await();

		assertThat(exceptions).isEmpty();
		assertThat(repository.findById(entity.id)).isEmpty();
	}

	@Test // DATAJDBC-493
	public void concurrentUpdateAndDeleteAll() throws Exception {

		CountDownLatch startLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for all threads to wait on.
		CountDownLatch doneLatch = new CountDownLatch(concurrencyEntities.size() + 1); // latch for main thread to wait on
		// until all threads are done.

		UnaryOperator<DummyEntity> updateAction = e -> {
			try {
				return repository.save(e);
			} catch (Exception ex) {
				// When the delete execution is complete, the Update execution throws an
				// IncorrectUpdateSemanticsDataAccessException.
				if (ex.getCause() instanceof IncorrectUpdateSemanticsDataAccessException) {
					return null;
				}
				throw ex;
			}
		};

		UnaryOperator<DummyEntity> deleteAction = e -> {
			repository.deleteAll();
			return null;
		};

		concurrencyEntities.forEach(e -> executeInParallel(startLatch, doneLatch, updateAction, e));
		executeInParallel(startLatch, doneLatch, deleteAction, entity);

		doneLatch.await();

		assertThat(exceptions).isEmpty();
		assertThat(repository.count()).isEqualTo(0);
	}

	private void executeInParallel(CountDownLatch startLatch, CountDownLatch doneLatch,
								   UnaryOperator<DummyEntity> deleteAction, DummyEntity entity) {
		// delete
		new Thread(() -> {
			try {

				startLatch.countDown();
				startLatch.await();

				transactionTemplate.execute(status -> deleteAction.apply(entity));
			} catch (Exception ex) {
				exceptions.add(ex);
			} finally {
				doneLatch.countDown();
			}
		}).start();
	}

	private List<DummyEntity> createEntityStates(DummyEntity entity) {

		List<DummyEntity> concurrencyEntities = new ArrayList<>();
		Element element1 = new Element(null, 1L);
		Element element2 = new Element(null, 2L);

		for (int i = 0; i < 50; i++) {

			List<Element> newContent = Arrays.asList(element1.withContent(element1.content + i + 2),
					element2.withContent(element2.content + i + 2));

			concurrencyEntities.add(entity.withName(entity.getName() + i).withContent(newContent));
		}
		return concurrencyEntities;
	}

	private static DummyEntity createDummyEntity() {
		return new DummyEntity(null, "Entity Name", new ArrayList<>());
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
	}

	static class DummyEntity {

		@Id
		private Long id;
		String name;
		final List<Element> content;

		public DummyEntity(Long id, String name, List<Element> content) {
			this.id = id;
			this.name = name;
			this.content = content;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public List<Element> getContent() {
			return this.content;
		}

		public DummyEntity withName(String name) {
			return this.name == name ? this : new DummyEntity(this.id, name, this.content);
		}

		public DummyEntity withContent(List<Element> content) {
			return this.content == content ? this : new DummyEntity(this.id, this.name, content);
		}
	}

	static class Element {

		@Id
		private Long id;
		final Long content;

		public Element(Long id, Long content) {
			this.id = id;
			this.content = content;
		}

		public Element withContent(Long content) {
			return this.content == content ? this : new Element(this.id, content);
		}
	}
}
