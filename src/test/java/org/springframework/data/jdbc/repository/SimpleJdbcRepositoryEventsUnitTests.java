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

import org.assertj.core.groups.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterLoadEvent;
import org.springframework.data.jdbc.mapping.event.AfterSave;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeSave;
import org.springframework.data.jdbc.mapping.event.Identifier;
import org.springframework.data.jdbc.mapping.event.JdbcEvent;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Unit tests for application events via {@link SimpleJdbcRepository}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	CollectingEventPublisher publisher = new CollectingEventPublisher();

	DummyEntityRepository repository;
	DefaultDataAccessStrategy dataAccessStrategy;

	@Before
	public void before() {

		JdbcMappingContext context = new JdbcMappingContext(createIdGeneratingOperations());

		dataAccessStrategy = spy(new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context), //
				context //
		));

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory( //
				publisher, //
				context, //
				dataAccessStrategy //
		);

		repository = factory.getRepository(DummyEntityRepository.class);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSave() {

		DummyEntity entity = new DummyEntity(23L);

		repository.save(entity);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeSave.class, //
						AfterSave.class //
		);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.saveAll(asList(entity1, entity2));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeSave.class, //
						AfterSave.class, //
						BeforeSave.class, //
						AfterSave.class //
		);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		assertThat(publisher.events).extracting( //
				JdbcEvent::getClass, //
				e -> e.getOptionalEntity().orElseGet(AssertionFailedError::new), //
				JdbcEvent::getId //
		).containsExactly( //
				Tuple.tuple(BeforeDelete.class, entity, Identifier.of(23L)), //
				Tuple.tuple(AfterDelete.class, entity, Identifier.of(23L)) //
		);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDeleteById() {

		repository.deleteById(23L);

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						BeforeDelete.class, //
						AfterDelete.class //
		);
	}

	@Test // DATAJDBC-197
	public void publishesEventsOnFindAll() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAll(any(Class.class));

		repository.findAll();

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterLoadEvent.class //
		);
	}

	@Test // DATAJDBC-197
	public void publishesEventsOnFindAllById() {

		DummyEntity entity1 = new DummyEntity(42L);
		DummyEntity entity2 = new DummyEntity(23L);

		doReturn(asList(entity1, entity2)).when(dataAccessStrategy).findAllById(any(Iterable.class), any(Class.class));

		repository.findAllById(asList(42L, 23L));

		assertThat(publisher.events) //
				.extracting(e -> (Class) e.getClass()) //
				.containsExactly( //
						AfterLoadEvent.class, //
						AfterLoadEvent.class //
		);
	}

	@Test // DATAJDBC-197
	public void publishesEventsOnFindById() {

		DummyEntity entity1 = new DummyEntity(23L);
		doReturn(entity1).when(dataAccessStrategy).findById(eq(23L), any(Class.class));

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
	@RequiredArgsConstructor
	static class DummyEntity {
		@Id Long id;
	}

	static class CollectingEventPublisher implements ApplicationEventPublisher {

		List<JdbcEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((JdbcEvent) o);
		}
	}
}
