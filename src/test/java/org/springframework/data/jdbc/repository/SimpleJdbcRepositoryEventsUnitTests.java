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
package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import junit.framework.AssertionFailedError;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import lombok.experimental.Wither;
import org.assertj.core.groups.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.relational.core.mapping.event.AfterSaveEvent;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.data.relational.core.mapping.event.RelationalEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Unit tests for application events via {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Oliver Gierke
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	CollectingEventPublisher publisher = new CollectingEventPublisher();

	DummyEntityRepository repository;
	DefaultDataAccessStrategy dataAccessStrategy;

	@Before
	public void before() {

		RelationalMappingContext context = new RelationalMappingContext();
		RelationalConverter converter = new BasicRelationalConverter(context, new JdbcCustomConversions());

		NamedParameterJdbcOperations operations = createIdGeneratingOperations();
		SqlGeneratorSource generatorSource = new SqlGeneratorSource(context);

		this.dataAccessStrategy = spy(
				new DefaultDataAccessStrategy(generatorSource, context, converter, operations));

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(dataAccessStrategy, context, converter, publisher,
				operations);

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
						BeforeSaveEvent.class, //
						AfterSaveEvent.class, //
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
				e -> e.getOptionalEntity().orElseGet(AssertionFailedError::new), //
				RelationalEvent::getId //
		).containsExactly( //
				Tuple.tuple(BeforeDeleteEvent.class, entity, Identifier.of(23L)), //
				Tuple.tuple(AfterDeleteEvent.class, entity, Identifier.of(23L)) //
		);
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
						AfterLoadEvent.class //
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
						AfterLoadEvent.class //
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
						AfterLoadEvent.class //
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

		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);
		when(operations.update(anyString(), any(SqlParameterSource.class), any(KeyHolder.class)))
				.thenAnswer(setIdInKeyHolder);
		return operations;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Value
	@Wither
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
