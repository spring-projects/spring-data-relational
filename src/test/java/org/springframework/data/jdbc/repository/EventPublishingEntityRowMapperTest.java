package org.springframework.data.jdbc.repository;

import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.event.AfterCreationEvent;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.jdbc.core.RowMapper;

import lombok.Data;

/**
 * @author Jens Schauder
 */
public class EventPublishingEntityRowMapperTest {

	private RowMapper rowMapperDelegate = mock(RowMapper.class);
	private JdbcPersistentEntity<DummyEntity> entity = mock(JdbcPersistentEntity.class);
	private ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

	@Test // DATAJDBC-99
	public void eventGetsPublishedAfterInstantiation() throws SQLException {

		when(entity.getIdValue(any())).thenReturn(1L);

		EventPublishingEntityRowMapper<DummyEntity> rowMapper = new EventPublishingEntityRowMapper<>(
				rowMapperDelegate,
				entity,
				publisher);

		ResultSet resultSet = mock(ResultSet.class);
		rowMapper.mapRow(resultSet, 1);

		verify(publisher).publishEvent(isA(AfterCreationEvent.class));
	}

	@Data
	private static class DummyEntity {

		@Id private final Long Id;
	}
}