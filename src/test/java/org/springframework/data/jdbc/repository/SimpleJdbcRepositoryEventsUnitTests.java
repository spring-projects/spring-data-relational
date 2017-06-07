package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
import org.springframework.data.jdbc.mapping.event.Identifier;
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

	FakePublisher publisher = new FakePublisher();

	DummyEntityRepository repository;

	@Before
	public void before() {

		NamedParameterJdbcOperations operations = createIdGeneratingOperations();
		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(operations, publisher);
		repository = factory.getRepository(DummyEntityRepository.class);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSave() {

		DummyEntity entity = new DummyEntity(23L);

		repository.save(entity);

		assertThat(publisher.events.get(0)).isInstanceOf(BeforeUpdate.class);
		assertThat(publisher.events.get(1)).isInstanceOf(AfterUpdate.class);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnSaveMany() {

		DummyEntity entity1 = new DummyEntity(null);
		DummyEntity entity2 = new DummyEntity(23L);

		repository.saveAll(asList(entity1, entity2));

		assertThat(publisher.events.get(0)).isInstanceOf(BeforeInsert.class);
		assertThat(publisher.events.get(1)).isInstanceOf(AfterInsert.class);
		assertThat(publisher.events.get(2)).isInstanceOf(BeforeUpdate.class);
		assertThat(publisher.events.get(3)).isInstanceOf(AfterUpdate.class);
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDelete() {

		DummyEntity entity = new DummyEntity(23L);

		repository.delete(entity);

		assertThat(publisher.events.get(0)).isInstanceOf(BeforeDelete.class);
		assertThat(publisher.events.get(1)).isInstanceOf(AfterDelete.class);

		assertThat(publisher.events.get(0).getOptionalEntity()).hasValue(entity);
		assertThat(publisher.events.get(1).getOptionalEntity()).hasValue(entity);

		assertThat(publisher.events.get(0).getId()).isEqualTo(Identifier.of(23L));
		assertThat(publisher.events.get(1).getId()).isEqualTo(Identifier.of(23L));
	}

	@Test // DATAJDBC-99
	public void publishesEventsOnDeleteById() {

		repository.deleteById(23L);

		assertThat(publisher.events.get(0)).isInstanceOf(BeforeDelete.class);
		assertThat(publisher.events.get(1)).isInstanceOf(AfterDelete.class);
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
