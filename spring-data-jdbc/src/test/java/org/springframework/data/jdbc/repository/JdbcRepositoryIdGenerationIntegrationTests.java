/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.jdbc.core.convert.IdGeneratingEntityCallback;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.Sequence;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Testing special cases for id generation with {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Mikhail Polivakha
 * @author Mark Paluch
 */
@IntegrationTest
class JdbcRepositoryIdGenerationIntegrationTests {

	@Autowired NamedParameterJdbcOperations operations;
	@Autowired ReadOnlyIdEntityRepository readOnlyIdRepository;
	@Autowired PrimitiveIdEntityRepository primitiveIdRepository;
	@Autowired ImmutableWithManualIdEntityRepository immutableWithManualIdEntityRepository;

	@Autowired SimpleSeqRepository simpleSeqRepository;
	@Autowired PersistableSeqRepository persistableSeqRepository;
	@Autowired PrimitiveIdSeqRepository primitiveIdSeqRepository;
	@Autowired IdGeneratingEntityCallback idGeneratingCallback;

	@Test // DATAJDBC-98
	void idWithoutSetterGetsSet() {

		ReadOnlyIdEntity entity = readOnlyIdRepository.save(new ReadOnlyIdEntity(null, "Entity Name"));

		assertThat(entity.id()).isNotNull();

		assertThat(readOnlyIdRepository.findById(entity.id())).hasValueSatisfying(it -> {

			assertThat(it.id()).isEqualTo(entity.id());
			assertThat(it.name()).isEqualTo(entity.name());
		});
	}

	@Test // DATAJDBC-98
	void primitiveIdGetsSet() {

		PrimitiveIdEntity entity = new PrimitiveIdEntity();
		entity.setName("Entity Name");

		PrimitiveIdEntity saved = primitiveIdRepository.save(entity);

		assertThat(saved.getId()).isNotEqualTo(0L);

		assertThat(primitiveIdRepository.findById(saved.getId())).hasValueSatisfying(it -> {

			assertThat(it.getId()).isEqualTo(saved.getId());
			assertThat(it.getName()).isEqualTo(saved.getName());
		});
	}

	@Test // DATAJDBC-393
	void manuallyGeneratedId() {

		ImmutableWithManualIdEntity entity = new ImmutableWithManualIdEntity(null, "immutable");
		ImmutableWithManualIdEntity saved = immutableWithManualIdEntityRepository.save(entity);

		assertThat(saved.id()).isNotNull();

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(1);
	}

	@Test // DATAJDBC-393
	void manuallyGeneratedIdForSaveAll() {

		ImmutableWithManualIdEntity one = new ImmutableWithManualIdEntity(null, "one");
		ImmutableWithManualIdEntity two = new ImmutableWithManualIdEntity(null, "two");
		List<ImmutableWithManualIdEntity> saved = immutableWithManualIdEntityRepository.saveAll(List.of(one, two));

		assertThat(saved).allSatisfy(e -> assertThat(e.id).isNotNull());

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(2);
	}

	@Test // DATAJDBC-2003
	@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SEQUENCES)
	void testUpdateAggregateWithSequence() {

		operations.getJdbcOperations().update("INSERT INTO SimpleSeq(id, name) VALUES(1, 'initial value')");

		SimpleSeq entity = new SimpleSeq();
		entity.id = 1L;
		entity.name = "New name";
		CompletableFuture<SimpleSeq> afterCallback = mockIdGeneratingCallback(entity);

		SimpleSeq updated = simpleSeqRepository.save(entity);

		assertThat(updated.id).isEqualTo(1L);
		assertThat(afterCallback.join().id).isEqualTo(1L);
	}

	@Test // DATAJDBC-2003
	@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SEQUENCES)
	void testInsertPersistableAggregateWithSequenceClientIdIsFavored() {

		long initialId = 1L;
		PersistableSeq entityWithSeq = PersistableSeq.createNew(initialId, "name");
		CompletableFuture<PersistableSeq> afterCallback = mockIdGeneratingCallback(entityWithSeq);

		PersistableSeq saved = persistableSeqRepository.save(entityWithSeq);

		// We do not expect the SELECT next value from sequence in case we're doing an INSERT with ID provided by the client
		assertThat(saved.getId()).isEqualTo(initialId);
		assertThat(afterCallback.join().id).isEqualTo(initialId);
	}

	@Test // DATAJDBC-2003
	@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SEQUENCES)
	void testInsertAggregateWithSequenceAndUnsetPrimitiveId() {

		PrimitiveIdSeq entity = new PrimitiveIdSeq();
		entity.name = "some name";
		CompletableFuture<PrimitiveIdSeq> afterCallback = mockIdGeneratingCallback(entity);

		PrimitiveIdSeq saved = primitiveIdSeqRepository.save(entity);

		// 1. Select from sequence
		// 2. Actual INSERT
		assertThat(afterCallback.join().id).isEqualTo(1L);
		assertThat(saved.id).isEqualTo(1L); // sequence starts with 1
	}

	@SuppressWarnings("unchecked")
	private <T> CompletableFuture<T> mockIdGeneratingCallback(T entity) {

		CompletableFuture<T> future = new CompletableFuture<>();

		Mockito.doAnswer(invocationOnMock -> {
			future.complete((T) invocationOnMock.callRealMethod());
			return future.join();
		}).when(idGeneratingCallback).onBeforeSave(Mockito.eq(entity), Mockito.any(MutableAggregateChange.class));

		return future;
	}

	interface PrimitiveIdEntityRepository extends ListCrudRepository<PrimitiveIdEntity, Long> {}

	interface ReadOnlyIdEntityRepository extends ListCrudRepository<ReadOnlyIdEntity, Long> {}

	interface ImmutableWithManualIdEntityRepository extends ListCrudRepository<ImmutableWithManualIdEntity, Long> {}

	interface SimpleSeqRepository extends ListCrudRepository<SimpleSeq, Long> {}

	interface PersistableSeqRepository extends ListCrudRepository<PersistableSeq, Long> {}

	interface PrimitiveIdSeqRepository extends ListCrudRepository<PrimitiveIdSeq, Long> {}

	record ReadOnlyIdEntity(@Id Long id, String name) {
	}

	static class SimpleSeq {

		@Id
		@Sequence(value = "simple_seq_seq") private Long id;

		private String name;
	}

	static class PersistableSeq implements Persistable<Long> {

		@Id
		@Sequence(value = "persistable_seq_seq") private Long id;

		private String name;

		@Transient private boolean isNew;

		@PersistenceCreator
		public PersistableSeq() {}

		public PersistableSeq(Long id, String name, boolean isNew) {
			this.id = id;
			this.name = name;
			this.isNew = isNew;
		}

		static PersistableSeq createNew(Long id, String name) {
			return new PersistableSeq(id, name, true);
		}

		@Override
		public Long getId() {
			return id;
		}

		@Override
		public boolean isNew() {
			return isNew;
		}
	}

	static class PrimitiveIdSeq {

		@Id
		@Sequence(value = "primitive_seq_seq") private long id;

		private String name;

	}

	static class PrimitiveIdEntity {

		@Id private long id;
		String name;

		public long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	record ImmutableWithManualIdEntity(@Id Long id, String name) {

		@Override
		public Long id() {
			return this.id;
		}

		public ImmutableWithManualIdEntity withId(Long id) {
			return Objects.equals(this.id, id) ? this : new ImmutableWithManualIdEntity(id, this.name);
		}

		public ImmutableWithManualIdEntity withName(String name) {
			return Objects.equals(this.name, name) ? this : new ImmutableWithManualIdEntity(this.id, name);
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	@Import(TestConfiguration.class)
	static class Config {

		AtomicLong lastId = new AtomicLong(0);

		/**
		 * {@link NamingStrategy} that harmlessly uppercases the table name, demonstrating how to inject one while not
		 * breaking existing SQL operations.
		 */
		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				@Override
				public String getTableName(Class<?> type) {
					return type.getSimpleName().toUpperCase();
				}
			};
		}

		@Bean
		BeforeConvertCallback<ImmutableWithManualIdEntity> idGenerator() {
			return e -> e.withId(lastId.incrementAndGet());
		}
	}
}
