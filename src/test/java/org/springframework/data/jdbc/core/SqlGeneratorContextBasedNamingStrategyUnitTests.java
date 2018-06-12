/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests to verify a contextual {@link NamingStrategy} implementation that customizes using a user-centric
 * {@link ThreadLocal}. NOTE: Due to the need to verify SQL generation and {@link SqlGenerator}'s package-private status
 * suggests this unit test exist in this package, not {@literal org.springframework.data.jdbc.mappings.model}.
 *
 * @author Greg Turnquist
 */
public class SqlGeneratorContextBasedNamingStrategyUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	ThreadLocal<String> userHandler = new ThreadLocal<>();

	/**
	 * Use a {@link NamingStrategy}, but override the schema with a {@link ThreadLocal}-based setting.
	 */
	private final NamingStrategy contextualNamingStrategy = new NamingStrategy() {

		@Override
		public String getSchema() {
			return userHandler.get();
		}
	};

	@Test // DATAJDBC-107
	public void findOne() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.getFindOne();

			SoftAssertions softAssertions = new SoftAssertions();
			softAssertions.assertThat(sql) //
					.startsWith("SELECT") //
					.contains(user + ".dummy_entity.id AS id,") //
					.contains(user + ".dummy_entity.name AS name,") //
					.contains("ref.l1id AS ref_l1id") //
					.contains("ref.content AS ref_content") //
					.contains("FROM " + user + ".dummy_entity");
			softAssertions.assertAll();
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteFirstLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteByPath(getPath("ref", DummyEntity.class));

			assertThat(sql).isEqualTo("DELETE FROM " + user + ".referenced_entity WHERE " + "dummy_entity = :rootId");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllSecondLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteByPath(getPath("ref.further", DummyEntity.class));

			assertThat(sql).isEqualTo( //
					"DELETE FROM " + user + ".second_level_referenced_entity " //
							+ "WHERE " + "referenced_entity IN " //
							+ "(SELECT l1id FROM " + user + ".referenced_entity " //
							+ "WHERE " + "dummy_entity = :rootId)");
		});
	}

	@Test // DATAJDBC-107
	public void deleteAll() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(null);

			assertThat(sql).isEqualTo("DELETE FROM " + user + ".dummy_entity");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllFirstLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(getPath("ref", DummyEntity.class));

			assertThat(sql).isEqualTo( //
					"DELETE FROM " + user + ".referenced_entity WHERE " + "dummy_entity IS NOT NULL");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteSecondLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further", DummyEntity.class));

			assertThat(sql).isEqualTo( //
					"DELETE FROM " + user + ".second_level_referenced_entity " //
							+ "WHERE " + "referenced_entity IN " //
							+ "(SELECT l1id FROM " + user + ".referenced_entity " //
							+ "WHERE " + "dummy_entity IS NOT NULL)");
		});
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path, Class<DummyEntity> baseType) {
		return PersistentPropertyPathTestUtils.getPath(this.context, path, baseType);
	}

	/**
	 * Take a set of user-based assertions and run them against multiple users, in different threads.
	 */
	private void testAgainstMultipleUsers(Consumer<String> testAssertions) {

		AtomicReference<Error> exception = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(2);

		threadedTest("User1", latch, testAssertions, exception);
		threadedTest("User2", latch, testAssertions, exception);

		try {
			if (!latch.await(10L, TimeUnit.SECONDS)) {
				fail("Test failed due to a time out.");
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		Error ex = exception.get();
		if (ex != null) {
			throw ex;
		}
	}

	/**
	 * Inside a {@link Runnable}, fetch the {@link ThreadLocal}-based username and execute the provided set of assertions.
	 * Then signal through the provided {@link CountDownLatch}.
	 */
	private void threadedTest(String user, CountDownLatch latch, Consumer<String> testAssertions,
			AtomicReference<Error> exception) {

		new Thread(() -> {

			try {

				userHandler.set(user);
				testAssertions.accept(user);

			} catch (Error ex) {
				exception.compareAndSet(null, ex);
			} finally {
				latch.countDown();
			}

		}).start();
	}

	/**
	 * Plug in a custom {@link NamingStrategy} for this test case.
	 */
	private SqlGenerator configureSqlGenerator(NamingStrategy namingStrategy) {

		RelationalMappingContext context = new RelationalMappingContext(namingStrategy);
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

		return new SqlGenerator(context, persistentEntity, new SqlGeneratorSource(context));
	}

	static class DummyEntity {

		@Id Long id;
		String name;
		ReferencedEntity ref;
	}

	static class ReferencedEntity {

		@Id Long l1id;
		String content;
		SecondLevelReferencedEntity further;
	}

	static class SecondLevelReferencedEntity {

		@Id Long l2id;
		String something;
	}
}
