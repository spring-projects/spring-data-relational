/*
 * Copyright 2017-2021 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultJdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.config.DialectResolver;
import org.springframework.data.jdbc.repository.config.DialectResolver.DefaultDialectProvider;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.*;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;

/**
 * Unit tests for application events via {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Oliver Gierke
 * @author Myeonghyeon Lee
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	CollectingEventPublisher publisher = new CollectingEventPublisher();

	DummyEntityRepository repository;
	DefaultDataAccessStrategy dataAccessStrategy;

	@BeforeEach
	public void before() {

		RelationalMappingContext context = new JdbcMappingContext();

		NamedParameterJdbcOperations operations = createIdGeneratingOperations();

		JdbcOperations mockJdbcOperations = operations.getJdbcOperations();

		when(mockJdbcOperations.execute(any(ConnectionCallback.class))).thenReturn(PostgresDialect.INSTANCE);

		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		Dialect dialect = HsqlDbDialect.INSTANCE;
		JdbcConverter converter = new BasicJdbcConverter(context, delegatingDataAccessStrategy, new JdbcCustomConversions(),
				new DefaultJdbcTypeFactory(mockJdbcOperations), dialect.getIdentifierProcessing());
		SqlGeneratorSource generatorSource = new SqlGeneratorSource(context, converter, dialect);

		this.dataAccessStrategy = spy(new DefaultDataAccessStrategy(generatorSource, context, converter, operations));
		delegatingDataAccessStrategy.setDelegate(dataAccessStrategy);
		doReturn(true).when(dataAccessStrategy).update(any(), any());

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(dataAccessStrategy, context, converter,
				H2Dialect.INSTANCE, publisher, operations);

		this.repository = factory.getRepository(DummyEntityRepository.class);
	}

	@Test // DATAJDBC-99
	@SuppressWarnings("rawtypes")
	public void publishesEventsOnSave() {

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
	public void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.saveAll(asList(entity1, entity2));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeConvertEvent.class, //
						BeforeSaveEvent.class, //
						AfterSaveEvent.class, //
						BeforeConvertEvent.class, //
						BeforeSaveEvent.class, //
						AfterSaveEvent.class //
				);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		assertThat(publisher.events).extracting( //
				RelationalEvent::getClass, //
				this::getEntity, //
				this::getId //
		).containsExactly( //
				Tuple.tuple(BeforeDeleteEvent.class, entity, Identifier.of(23L)), //
				Tuple.tuple(AfterDeleteEvent.class, entity, Identifier.of(23L)) //
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
	public void publishesEventsOnDeleteById() {

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
	public void publishesEventsOnFindAll() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any());

		repository.findAll();

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterConvertEvent.class, //
						AfterLoadEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-197
	@SuppressWarnings("rawtypes")
	public void publishesEventsOnFindAllById() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAllById(any(), any());

		repository.findAllById(asList(42L, 23L));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterConvertEvent.class, //
						AfterLoadEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-197
	@SuppressWarnings("rawtypes")
	public void publishesEventsOnFindById() {

		DummyEntity entity1 = new DummyEntity(23L);

		doReturn(entity1).when(dataAccessStrategy).findById(eq(23L), any());

		repository.findById(23L);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-101
	@SuppressWarnings("rawtypes")
	public void publishesEventsOnFindAllSorted() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any(), any(Sort.class));

		repository.findAll(Sort.by("field"));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterConvertEvent.class, //
						AfterLoadEvent.class, //
						AfterConvertEvent.class //
				);
	}

	@Test // DATAJDBC-101
	@SuppressWarnings("rawtypes")
	public void publishesEventsOnFindAllPaged() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any(), any(Pageable.class));
		doReturn(2L).when(dataAccessStrategy).count(any());

		repository.findAll(PageRequest.of(0, 20));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterConvertEvent.class, //
						AfterLoadEvent.class, //
						AfterConvertEvent.class //
				);
	}

	private static NamedParameterJdbcOperations createIdGeneratingOperations() {

		Answer<Integer> setIdInKeyHolder = invocation -> {

			HashMap<String, Object> keys = new HashMap<>();
			keys.put("id", 4711L);
			KeyHolder keyHolder = invocation.getArgument(2);
			keyHolder.getKeyList().add(keys);

			return 1;
		};

		JdbcOperations mockJdbcOperations = mock(JdbcOperations.class);

		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);
		when(operations.update(anyString(), any(SqlParameterSource.class), any(KeyHolder.class)))
				.thenAnswer(setIdInKeyHolder);
		when(operations.getJdbcOperations()).thenReturn(mockJdbcOperations);
		return operations;
	}

	interface DummyEntityRepository extends PagingAndSortingRepository<DummyEntity, Long> {}

	@Value
	@With
	@RequiredArgsConstructor
	static class DummyEntity {
		@Id Long id;
	}

	static class CollectingEventPublisher implements ApplicationEventPublisher {

		List<RelationalEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((RelationalEvent) o);
		}
	}
}
