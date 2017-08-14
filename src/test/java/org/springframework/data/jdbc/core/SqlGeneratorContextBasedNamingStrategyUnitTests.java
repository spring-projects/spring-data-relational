/*
 * Copyright 2017 the original author or authors.
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
import java.util.function.Consumer;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.model.DefaultNamingStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.NamingStrategy;
import org.springframework.data.mapping.PropertyPath;

/**
 * Unit tests to verify a contextual {@link NamingStrategy} implementation that customizes using a user-centric {@link ThreadLocal}.
 *
 * NOTE: Due to the need to verify SQL generation and {@link SqlGenerator}'s package-private status suggests
 * this unit test exist in this package, not {@literal org.springframework.data.jdbc.mappings.model}.
 *
 * @author Greg Turnquist
 */
public class SqlGeneratorContextBasedNamingStrategyUnitTests {

	private final ThreadLocal<String> userHandler = new ThreadLocal<>();

	/**
	 * Use a {@link DefaultNamingStrategy}, but override the schema with a {@link ThreadLocal}-based setting.
	 */
	private final NamingStrategy contextualNamingStrategy = new DefaultNamingStrategy() {
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
				.contains(user + ".DummyEntity.id AS id,") //
				.contains(user + ".DummyEntity.name AS name,") //
				.contains("ref.l1id AS ref_l1id") //
				.contains("ref.content AS ref_content") //
				.contains("FROM " + user + ".DummyEntity");
			softAssertions.assertAll();
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteFirstLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteByPath(PropertyPath.from("ref", DummyEntity.class));

			assertThat(sql).isEqualTo(
				"DELETE FROM " + user + ".ReferencedEntity WHERE " + user + ".DummyEntity = :rootId");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllSecondLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteByPath(PropertyPath.from("ref.further", DummyEntity.class));

			assertThat(sql).isEqualTo(
				"DELETE FROM " + user + ".SecondLevelReferencedEntity " +
					"WHERE " + user + ".ReferencedEntity IN " +
						"(SELECT l1id FROM " + user + ".ReferencedEntity " +
						"WHERE " + user + ".DummyEntity = :rootId)");
		});
	}

	@Test // DATAJDBC-107
	public void deleteAll() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(null);

			assertThat(sql).isEqualTo("DELETE FROM " + user + ".DummyEntity");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllFirstLevel() {

		testAgainstMultipleUsers(user -> {

			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(PropertyPath.from("ref", DummyEntity.class));

			assertThat(sql).isEqualTo(
				"DELETE FROM " + user + ".ReferencedEntity WHERE " + user + ".DummyEntity IS NOT NULL");
		});
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteSecondLevel() {

		testAgainstMultipleUsers(user -> {
			
			SqlGenerator sqlGenerator = configureSqlGenerator(contextualNamingStrategy);

			String sql = sqlGenerator.createDeleteAllSql(PropertyPath.from("ref.further", DummyEntity.class));

			assertThat(sql).isEqualTo(
				"DELETE FROM " + user + ".SecondLevelReferencedEntity " +
				"WHERE " + user + ".ReferencedEntity IN " +
					"(SELECT l1id FROM " + user + ".ReferencedEntity " +
					"WHERE " + user + ".DummyEntity IS NOT NULL)");
		});
	}

	/**
	 * Take a set of user-based assertions and run them against multiple users, in different threads.
	 */
	private void testAgainstMultipleUsers(Consumer<String> testAssertions) {

		CountDownLatch latch = new CountDownLatch(2);

		threadedTest("User1", latch, testAssertions);
		threadedTest("User2", latch, testAssertions);

		try {
			latch.await(10L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Inside a {@link Runnable}, fetch the {@link ThreadLocal}-based username and execute the provided
	 * set of assertions. Then signal through the provided {@link CountDownLatch}.
	 */
	private void threadedTest(String user, CountDownLatch latch, Consumer<String> testAssertions) {

		new Thread(() -> {
			userHandler.set(user);

			testAssertions.accept(user);

			latch.countDown();
		}).start();
	}

	/**
	 * Plug in a custom {@link NamingStrategy} for this test case.
	 */
	private SqlGenerator configureSqlGenerator(NamingStrategy namingStrategy) {

		JdbcMappingContext context = new JdbcMappingContext(namingStrategy);
		JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		
		return new SqlGenerator(context, persistentEntity, new SqlGeneratorSource(context));
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Id Long id;
		String name;
		ReferencedEntity ref;
	}

	@SuppressWarnings("unused")
	static class ReferencedEntity {

		@Id Long l1id;
		String content;
		SecondLevelReferencedEntity further;
	}

	@SuppressWarnings("unused")
	static class SecondLevelReferencedEntity {

		@Id Long l2id;
		String something;
	}

}
