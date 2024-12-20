/*
 * Copyright 2017-2024 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.convert.*;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.AfterConvertEvent;
import org.springframework.data.relational.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.relational.core.mapping.event.AfterSaveEvent;
import org.springframework.data.relational.core.mapping.event.BeforeConvertEvent;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.data.relational.core.mapping.event.RelationalEvent;
import org.springframework.data.relational.core.mapping.event.WithId;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;


/**
 * Unit tests for application events via {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Oliver Gierke
 * @author Myeonghyeon Lee
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @author Mikhail Polivakha
 */
class SimpleJdbcRepositoryEventsUnitTests {

	private static final long generatedId = 4711L;

	private final CollectingEventPublisher publisher = new CollectingEventPublisher();

	private DummyEntityRepository repository;
	private DefaultDataAccessStrategy dataAccessStrategy;

	@BeforeEach
	void before() {

		RelationalMappingContext context = new JdbcMappingContext();
		NamedParameterJdbcOperations operations = createIdGeneratingOperations();

		Dialect dialect = HsqlDbDialect.INSTANCE;
		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		JdbcConverter converter = new MappingJdbcConverter(context, delegatingDataAccessStrategy,
				new JdbcCustomConversions(), new DefaultJdbcTypeFactory(operations.getJdbcOperations()));
		SqlGeneratorSource generatorSource = new SqlGeneratorSource(context, converter, dialect);
		SqlParametersFactory sqlParametersFactory = new SqlParametersFactory(context, converter);
		InsertStrategyFactory insertStrategyFactory = new InsertStrategyFactory(operations, dialect);

		this.dataAccessStrategy = spy(new DefaultDataAccessStrategy(generatorSource, context, converter, operations,
				sqlParametersFactory, insertStrategyFactory, QueryMappingConfiguration.EMPTY));
		delegatingDataAccessStrategy.setDelegate(dataAccessStrategy);
		doReturn(true).when(dataAccessStrategy).update(any(), any());

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(dataAccessStrategy, context, converter,
				H2Dialect.INSTANCE, publisher, operations);

		this.repository = factory.getRepository(DummyEntityRepository.class);
	}

	@Test // DATAJDBC-99
	@SuppressWarnings("rawtypes")
	void publishesEventsOnSave() {

		DummyEntity entity = new DummyEntity(23L);

		repository.save(entity);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeConvertEvent.class, //
						BeforeSaveEvent.class, //
						AfterSaveEvent.class //
				);
	}

	@Test // DATAJDBC-99
	@SuppressWarnings("rawtypes")
	void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.saveAll(asList(entity1, entity2));

		assertThat(publisher.events) //
				.extracting(RelationalEvent::getClass, e -> ((DummyEntity) e.getEntity()).getId()) //
				.containsExactly( //
						tuple(BeforeConvertEvent.class, null), //
						tuple(BeforeSaveEvent.class, null), //
						tuple(BeforeConvertEvent.class, 23L), //
						tuple(BeforeSaveEvent.class, 23L), //
						tuple(AfterSaveEvent.class, generatedId), //
						tuple(AfterSaveEvent.class, 23L) //
				);
	}

	@Test // DATAJDBC-99
	void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		assertThat(publisher.events).extracting( //
				RelationalEvent::getClass, //
				this::getEntity, //
				this::getId //
		).containsExactly( //
				tuple(BeforeDeleteEvent.class, entity, Identifier.of(23L)), //
				tuple(AfterDeleteEvent.class, entity, Identifier.of(23L)) //
		);
	}

	private Identifier getId(RelationalEvent e) {
		return ((WithId) e).getId();
	}

	@Nullable
	private Object getEntity(RelationalEvent e) {
		return e.getEntity();
	}

	@Test // DATAJDBC-99
	@SuppressWarnings("rawtypes")
	void publishesEventsOnDeleteById() {

		repository.deleteById(23L);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeDeleteEvent.class, //
						AfterDeleteEvent.class //
				);
	}

	@Test // DATAJDBC-197
	@SuppressWarnings("rawtypes")
	void publishesEventsOnFindAll() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any());

		repository.findAll();

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterConvertEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-197
	@SuppressWarnings("rawtypes")
	void publishesEventsOnFindAllById() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAllById(any(), any());

		repository.findAllById(asList(42L, 23L));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterConvertEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-197
	@SuppressWarnings("rawtypes")
	void publishesEventsOnFindById() {

		DummyEntity entity1 = new DummyEntity(23L);

		doReturn(entity1).when(dataAccessStrategy).findById(eq(23L), any());

		repository.findById(23L);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-101
	@SuppressWarnings("rawtypes")
	void publishesEventsOnFindAllSorted() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any(), any(Sort.class));

		repository.findAll(Sort.by("field"));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterConvertEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-101
	@SuppressWarnings("rawtypes")
	void publishesEventsOnFindAllPaged() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any(), any(Pageable.class));
		doReturn(2L).when(dataAccessStrategy).count(any());

		repository.findAll(PageRequest.of(0, 20));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterConvertEvent.class, //
						AfterConvertEvent.class //
				);
	}

	private static NamedParameterJdbcOperations createIdGeneratingOperations() {

		Answer<Integer> setIdInKeyHolder = invocation -> {

			HashMap<String, Object> keys = new HashMap<>();
			keys.put("id", generatedId);
			KeyHolder keyHolder = invocation.getArgument(2);
			keyHolder.getKeyList().add(keys);

			return 1;
		};

		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);
		when(operations.update(anyString(), any(SqlParameterSource.class), any(KeyHolder.class)))
				.thenAnswer(setIdInKeyHolder);
		when(operations.getJdbcOperations()).thenReturn(mock(JdbcOperations.class));
		return operations;
	}

	interface DummyEntityRepository
			extends CrudRepository<DummyEntity, Long>, PagingAndSortingRepository<DummyEntity, Long> {}

	static final class DummyEntity {
		private final @Id Long id;

		public DummyEntity(Long id) {
			this.id = id;
		}

		public Long getId() {
			return this.id;
		}

		public DummyEntity withId(Long id) {
			return this.id == id ? this : new DummyEntity(id);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			DummyEntity that = (DummyEntity) o;

			return ObjectUtils.nullSafeEquals(id, that.id);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(id);
		}

		public String toString() {
			return "SimpleJdbcRepositoryEventsUnitTests.DummyEntity(id=" + this.getId() + ")";
		}

	}

	static class CollectingEventPublisher implements ApplicationEventPublisher {

		List<RelationalEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((RelationalEvent) o);
		}
	}
}
