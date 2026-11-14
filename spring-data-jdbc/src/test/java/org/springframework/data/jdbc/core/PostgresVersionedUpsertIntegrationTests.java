/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.jdbc.core.convert.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.JdbcHsqlDbDialect;
import org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategy;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.core.sql.render.UpsertRenderContext;
import org.springframework.data.relational.core.sql.render.UpsertRenderContexts;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Integration tests for optimistic locking in PostgreSQL upserts.
 *
 * @author YeongJae Min
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.POSTGRES)
@ContextConfiguration(classes = PostgresVersionedUpsertIntegrationTests.Config.class)
class PostgresVersionedUpsertIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired PlatformTransactionManager transactionManager;

	@Test // GH-2277
	void upsertInitializesWrapperVersion() {

		Account inserted = template.upsert(new Account(1L, null, "inserted"));

		assertThat(inserted).isEqualTo(new Account(1L, 0L, "inserted"));
		assertThat(template.findById(1L, Account.class)).isEqualTo(inserted);
	}

	@Test // GH-2277
	void upsertIncrementsVersionAndReturnsTheStoredVersion() {

		Account inserted = template.insert(new Account(1L, null, "original"));

		Account updated = template.upsert(new Account(inserted.id(), inserted.version(), "updated"));

		assertThat(updated).isEqualTo(new Account(1L, 1L, "updated"));
		assertThat(template.findById(1L, Account.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	void upsertRejectsASecondRequestWithTheSameVersion() {

		Account inserted = template.insert(new Account(1L, null, "original"));
		Account updated = template.upsert(new Account(1L, inserted.version(), "updated"));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> template.upsert(new Account(1L, inserted.version(), "stale")));
		assertThat(template.findById(1L, Account.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	void upsertInitializesAndIncrementsPrimitiveVersion() {

		PrimitiveAccount inserted = template.upsert(new PrimitiveAccount(1L, 0, "inserted"));
		assertThat(inserted).isEqualTo(new PrimitiveAccount(1L, 1, "inserted"));
		assertThat(template.findById(1L, PrimitiveAccount.class)).isEqualTo(inserted);

		PrimitiveAccount updated = template.upsert(new PrimitiveAccount(1L, inserted.version(), "updated"));
		assertThat(updated).isEqualTo(new PrimitiveAccount(1L, 2, "updated"));
		assertThat(template.findById(1L, PrimitiveAccount.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	void upsertInitializesAndIncrementsIntegerVersionOnMutableEntity() {

		MutableAccount account = new MutableAccount();
		account.id = 1L;
		account.name = "inserted";

		assertThat(template.upsert(account).version).isZero();
		assertThat(template.findById(1L, MutableAccount.class).version).isZero();

		account.name = "updated";
		assertThat(template.upsert(account).version).isEqualTo(1);
		MutableAccount loaded = template.findById(1L, MutableAccount.class);
		assertThat(loaded.version).isEqualTo(1);
		assertThat(loaded.name).isEqualTo("updated");
	}

	@ParameterizedTest // GH-2277
	@CsvSource({ "0, 1", "5, 6" })
	void upsertInsertsWithNextVersionWhenRowDoesNotExist(long version, long expectedVersion) {

		Account inserted = template.upsert(new Account(1L, version, "inserted"));

		assertThat(inserted.version()).isEqualTo(expectedVersion);
		assertThat(template.findById(1L, Account.class)).isEqualTo(inserted);
	}

	@Test // GH-2277
	void upsertReinsertsDeletedEntityWithNextVersion() {

		Account inserted = template.upsert(new Account(1L, null, "inserted"));
		template.delete(inserted);

		Account reinserted = template.upsert(inserted);

		assertThat(reinserted).isEqualTo(new Account(1L, 1L, "inserted"));
		assertThat(template.findById(1L, Account.class)).isEqualTo(reinserted);
	}

	@ParameterizedTest // GH-2277
	@ValueSource(longs = { -1, 1, 5 })
	void upsertRejectsMismatchedVersions(long version) {

		Account inserted = template.upsert(new Account(1L, null, "original"));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> template.upsert(new Account(1L, version, "conflict")));
		assertThat(template.findById(1L, Account.class)).isEqualTo(inserted);
	}

	@Test // GH-2277
	void upsertWithNewWrapperVersionCannotOverwriteAnExistingRow() {

		Account inserted = template.upsert(new Account(1L, null, "original"));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> template.upsert(new Account(1L, null, "conflict")));
		assertThat(template.findById(1L, Account.class)).isEqualTo(inserted);
	}

	@Test // GH-2277
	void upsertWithNewPrimitiveVersionCannotOverwriteAnExistingRow() {

		// Wrapper version zero is a stored version, but primitive zero represents a new entity.
		Account inserted = template.upsert(new Account(1L, null, "original"));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> template.upsert(new PrimitiveAccount(1L, 0, "conflict")));
		assertThat(template.findById(1L, Account.class)).isEqualTo(inserted);
	}

	@Test // GH-2277
	void versionConflictPreservesReferencedEntities() {

		Basket inserted = template.upsert(new Basket(1L, null, "original", List.of(new Line("original"))));
		Basket updated = template.upsert(new Basket(1L, inserted.version(), "updated", List.of(new Line("updated"))));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> template.upsert(new Basket(1L, inserted.version(), "stale", List.of(new Line("stale")))));
		assertThat(template.findById(1L, Basket.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	@Transactional(propagation = Propagation.NOT_SUPPORTED)
	void childInsertFailureRollsBackRootVersionAndReferencedEntities() {

		TransactionTemplate transaction = new TransactionTemplate(transactionManager);
		Basket original = new Basket(1L, 0L, "original", List.of(new Line("original")));
		transaction.executeWithoutResult(status -> template.upsert(new Basket(1L, null, "original", original.lines())));

		try {
			assertThatExceptionOfType(DataIntegrityViolationException.class).isThrownBy(() -> transaction
					.executeWithoutResult(status -> template.upsert(new Basket(1L, 0L, "updated", List.of(new Line(null))))));

			Basket loaded = transaction.execute(status -> template.findById(1L, Basket.class));
			assertThat(loaded).isEqualTo(original);
		} finally {
			transaction.executeWithoutResult(status -> template.deleteById(1L, Basket.class));
		}
	}

	@Test // GH-2277
	@Transactional(propagation = Propagation.NOT_SUPPORTED)
	void concurrentUpsertsWithSameVersionHaveExactlyOneWinner() throws Exception {

		TransactionTemplate transaction = new TransactionTemplate(transactionManager);
		transaction.executeWithoutResult(status -> template.upsert(new Account(1L, null, "original")));
		ExecutorService executor = Executors.newFixedThreadPool(2);
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);

		try {
			Future<Boolean> first = executor.submit(() -> concurrentUpsert("first", ready, start));
			Future<Boolean> second = executor.submit(() -> concurrentUpsert("second", ready, start));
			assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
			start.countDown();

			assertThat(List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS)))
					.containsExactlyInAnyOrder(true, false);
			Account loaded = template.findById(1L, Account.class);
			assertThat(loaded.version()).isEqualTo(1L);
			assertThat(loaded.name()).isIn("first", "second");
		} finally {
			start.countDown();
			executor.shutdownNow();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
			transaction.executeWithoutResult(status -> template.deleteById(1L, Account.class));
		}
	}

	private boolean concurrentUpsert(String name, CountDownLatch ready, CountDownLatch start)
			throws InterruptedException {

		ready.countDown();
		assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
		try {
			new TransactionTemplate(transactionManager)
					.executeWithoutResult(status -> template.upsert(new Account(1L, 0L, name)));
			return true;
		} catch (OptimisticLockingFailureException ex) {
			return false;
		}
	}

	@Test // GH-2277
	void strategyDelegationPropagatesVersionConflict() {

		DataAccessStrategy strategy = template.getDataAccessStrategy();
		JdbcAggregateTemplate delegating = new JdbcAggregateTemplate(template.getConverter(),
				new DelegatingDataAccessStrategy(new CascadingDataAccessStrategy(List.of(strategy, strategy))));
		Account inserted = delegating.upsert(new Account(1L, null, "original"));
		Account updated = delegating.upsert(new Account(1L, inserted.version(), "updated"));

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> delegating.upsert(new Account(1L, inserted.version(), "stale")));
		assertThat(template.findById(1L, Account.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	void unsupportedDialectRejectsVersionedUpsert() {

		DataAccessStrategy strategy = new DataAccessStrategyFactory(template.getConverter(),
				template.getDataAccessStrategy().getJdbcOperations(), JdbcHsqlDbDialect.INSTANCE,
				QueryMappingConfiguration.EMPTY).create();
		JdbcAggregateTemplate unsupported = new JdbcAggregateTemplate(template.getConverter(), strategy);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> unsupported.upsert(new Account(1L, null, "inserted")));
		assertThat(template.findById(1L, Account.class)).isNull();
	}

	@Test // GH-2277
	void myBatisCombinationPreservesUnsupportedVersionedUpsertException() {

		var converter = template.getConverter();
		DataAccessStrategy strategy = MyBatisDataAccessStrategy.createCombinedAccessStrategy(converter.getMappingContext(),
				converter, template.getDataAccessStrategy().getJdbcOperations(), mock(SqlSession.class),
				JdbcHsqlDbDialect.INSTANCE, QueryMappingConfiguration.EMPTY);
		JdbcAggregateTemplate unsupported = new JdbcAggregateTemplate(converter, strategy);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> unsupported.upsert(new Account(1L, null, "inserted")));
		assertThat(template.findById(1L, Account.class)).isNull();
	}

	@ParameterizedTest // GH-2277
	@ValueSource(booleans = { false, true })
	void dialectWithoutUpsertReportsUnsupportedOperation(boolean combinedWithMyBatis) {

		var converter = template.getConverter();
		var jdbc = template.getDataAccessStrategy().getJdbcOperations();
		var dialect = new NoUpsertDialect();
		DataAccessStrategy strategy = combinedWithMyBatis
				? MyBatisDataAccessStrategy.createCombinedAccessStrategy(converter.getMappingContext(), converter, jdbc,
						mock(SqlSession.class), dialect, QueryMappingConfiguration.EMPTY)
				: new DataAccessStrategyFactory(converter, jdbc, dialect, QueryMappingConfiguration.EMPTY).create();
		JdbcAggregateTemplate unsupported = new JdbcAggregateTemplate(converter, strategy);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> unsupported.upsert(new Account(1L, null, "inserted")));
		assertThat(template.findById(1L, Account.class)).isNull();
	}

	@Test // GH-2277
	void aliasedConditionalUpsertUpdatesOnlyTheMatchingVersion() {

		template.upsert(new Account(1L, null, "original"));
		var table = SQL.table("versioned_account").as("a");
		var id = table.column("id");
		var version = table.column("version");
		var name = table.column("name");
		var upsert = StatementBuilder.upsert(table)
				.insert(id.set(SQL.bindMarker(":id")), version.set(SQL.bindMarker(":nextVersion")),
						name.set(SQL.bindMarker(":name")))
				.onConflict(it -> it.with(id).update(version, name))
				.where(version.isEqualTo(SQL.bindMarker(":previousVersion"))).build();
		DataAccessStrategy strategy = template.getDataAccessStrategy();
		String sql = SqlRenderer.create(new RenderContextFactory(strategy.getDialect()).createRenderContext())
				.render(upsert);
		var jdbc = strategy.getJdbcOperations();

		assertThat(jdbc.update(sql, Map.of("id", 1L, "nextVersion", 1L, "name", "updated", "previousVersion", 0L)))
				.isEqualTo(1);
		Account updated = new Account(1L, 1L, "updated");
		assertThat(template.findById(1L, Account.class)).isEqualTo(updated);

		assertThat(jdbc.update(sql, Map.of("id", 1L, "nextVersion", 2L, "name", "stale", "previousVersion", 0L))).isZero();
		assertThat(template.findById(1L, Account.class)).isEqualTo(updated);
	}

	@Test // GH-2277
	void upsertWithoutVersionRetainsExistingBehavior() {

		template.upsert(new UnversionedAccount(1L, "inserted"));
		UnversionedAccount updated = template.upsert(new UnversionedAccount(1L, "updated"));

		assertThat(template.findById(1L, UnversionedAccount.class)).isEqualTo(updated);
	}

	@Table("versioned_account")
	record Account(@Id Long id, @Version Long version, String name) {
	}

	@Table("versioned_account")
	record PrimitiveAccount(@Id Long id, @Version long version, String name) {
	}

	@Table("versioned_account")
	static class MutableAccount {
		@Id Long id;
		@Version Integer version;
		String name;
	}

	@Table("unversioned_account")
	record UnversionedAccount(@Id Long id, String name) {
	}

	@Table("versioned_account")
	record Basket(@Id Long id, @Version Long version, String name,
			@MappedCollection(idColumn = "account", keyColumn = "position") List<Line> lines) {
	}

	@Table("account_line")
	record Line(String description) {
	}

	static class NoUpsertDialect extends JdbcHsqlDbDialect {

		@Override
		public UpsertRenderContext getUpsertRenderContext() {
			return UpsertRenderContexts.UNSUPPORTED;
		}
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		TestClass testClass() {
			return TestClass.of(PostgresVersionedUpsertIntegrationTests.class);
		}
	}
}
