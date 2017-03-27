package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.util.Assert.*;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterInsert;
import org.springframework.data.jdbc.mapping.event.AfterUpdate;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeInsert;
import org.springframework.data.jdbc.mapping.event.BeforeUpdate;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.event.JdbcEvent;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Jens Schauder
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	private FakePublisher publisher = new FakePublisher();

	private DummyEntityRepository repository;

	@Before
	public void before() {

		NamedParameterJdbcOperations operations = createIdGeneratingOperations();
		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(operations, publisher);
		repository = factory.getRepository(DummyEntityRepository.class);
	}

	private NamedParameterJdbcOperations createIdGeneratingOperations() {

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

	@Test // DATAJDBC-99
	public void publishesEventsOnSave() {

		DummyEntity entity = new DummyEntity(23L);

		repository.save(entity);

		isInstanceOf(BeforeUpdate.class, publisher.events.get(0));
		isInstanceOf(AfterUpdate.class, publisher.events.get(1));
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.save(asList(entity1, entity2));

		isInstanceOf(BeforeInsert.class, publisher.events.get(0));
		isInstanceOf(AfterInsert.class, publisher.events.get(1));
		isInstanceOf(BeforeUpdate.class, publisher.events.get(2));
		isInstanceOf(AfterUpdate.class, publisher.events.get(3));
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		isInstanceOf(BeforeDelete.class, publisher.events.get(0));
		isInstanceOf(AfterDelete.class, publisher.events.get(1));

		assertEquals(entity, publisher.events.get(0).getOptionalEntity().get());
		assertEquals(entity, publisher.events.get(1).getOptionalEntity().get());

		assertEquals(new Specified(23L), publisher.events.get(0).getId());
		assertEquals(new Specified(23L), publisher.events.get(1).getId());
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDeleteById() {

		repository.delete(23L);

		isInstanceOf(BeforeDelete.class, publisher.events.get(0));
		isInstanceOf(AfterDelete.class, publisher.events.get(1));
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	private static class DummyEntity {
		@Id private final Long id;
	}

	static class FakePublisher implements ApplicationEventPublisher {

		List<JdbcEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((JdbcEvent) o);
		}
	}
}
