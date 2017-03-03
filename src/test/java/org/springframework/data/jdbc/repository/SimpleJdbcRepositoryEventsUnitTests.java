package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.util.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.event.AfterDeleteEvent;
import org.springframework.data.jdbc.mapping.event.AfterInsertEvent;
import org.springframework.data.jdbc.mapping.event.AfterUpdateEvent;
import org.springframework.data.jdbc.mapping.event.BeforeDeleteEvent;
import org.springframework.data.jdbc.mapping.event.BeforeInsertEvent;
import org.springframework.data.jdbc.mapping.event.BeforeUpdateEvent;
import org.springframework.data.jdbc.mapping.event.JdbcEvent;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import lombok.Data;

/**
 * @author Jens Schauder
 */
public class SimpleJdbcRepositoryEventsUnitTests {

	private FakePublisher publisher = new FakePublisher();

	private DummyEntityRepository repository;

	@Before
	public void before() {
		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(publisher, mock(NamedParameterJdbcOperations.class));
		repository = factory.getRepository(DummyEntityRepository.class);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSave() {

		DummyEntity entity = new DummyEntity(23L);

		repository.save(entity);

		isInstanceOf(BeforeUpdateEvent.class, publisher.events.get(0));
		isInstanceOf(AfterUpdateEvent.class, publisher.events.get(1));
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.save(asList(entity1, entity2));

		isInstanceOf(BeforeInsertEvent.class, publisher.events.get(0));
		isInstanceOf(AfterInsertEvent.class, publisher.events.get(1));
		isInstanceOf(BeforeUpdateEvent.class, publisher.events.get(2));
		isInstanceOf(AfterUpdateEvent.class, publisher.events.get(3));
	}


	@Test // DATAJDBC-99
	public void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		isInstanceOf(BeforeDeleteEvent.class, publisher.events.get(0));
		isInstanceOf(AfterDeleteEvent.class, publisher.events.get(1));

		assertEquals(entity, publisher.events.get(0).getInstance());
		assertEquals(entity, publisher.events.get(1).getInstance());

		assertEquals(23L, publisher.events.get(0).getId());
		assertEquals(23L, publisher.events.get(1).getId());
	}


	@Test // DATAJDBC-99
	public void publishesEventsOnDeleteById() {

		repository.delete(23L);

		isInstanceOf(BeforeDeleteEvent.class, publisher.events.get(0));
		isInstanceOf(AfterDeleteEvent.class, publisher.events.get(1));
	}


	@Data
	private static class DummyEntity {

		@Id private final Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

	static class FakePublisher implements ApplicationEventPublisher {

		List<JdbcEvent> events = new ArrayList<>();

		@Override
		public void publishEvent(Object o) {
			events.add((JdbcEvent) o);
		}
	}
}