package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import junit.framework.AssertionFailedError;
import lombok.Data;

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
 * @author Jens Schauder
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	FakePublisher publisher = new FakePublisher();

	DummyEntityRepository repository;

	@Before
	public void before() {

		final JdbcMappingContext context = new JdbcMappingContext(createIdGeneratingOperations());
		JdbcRepositoryFactory factory = new JdbcRepositoryFactory( //
				publisher, //
				context, //
				new DefaultDataAccessStrategy( //
						new SqlGeneratorSource(context), //
						context //
				) //
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
				e -> (Class) e.getClass(), //
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

	@Data
	static class DummyEntity {
		private final @Id Long id;
	}

	static class FakePublisher implements ApplicationEventPublisher {

		List<JdbcEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((JdbcEvent) o);
		}
	}
}
