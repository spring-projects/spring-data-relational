package org.springframework.data.jdbc.repository;

import static org.mockito.Mockito.*;

import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.event.AfterCreation;
import org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation;
import org.springframework.jdbc.core.RowMapper;

/**
 * @author Jens Schauder
 */
public class EventPublishingEntityRowMapperTest {

	RowMapper<DummyEntity> rowMapperDelegate = mock(RowMapper.class);
	JdbcPersistentEntityInformation<DummyEntity, Long> entityInformation = mock(JdbcPersistentEntityInformation.class);
	ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

	@Test // DATAJDBC-99
	public void eventGetsPublishedAfterInstantiation() throws SQLException {

		when(rowMapperDelegate.mapRow(any(ResultSet.class), anyInt())).thenReturn(new DummyEntity(1L));
		when(entityInformation.getId(any())).thenReturn(1L);

		EventPublishingEntityRowMapper rowMapper = new EventPublishingEntityRowMapper<>( //
				rowMapperDelegate, //
				entityInformation, //
				publisher);

		ResultSet resultSet = mock(ResultSet.class);
		rowMapper.mapRow(resultSet, 1);

		verify(publisher).publishEvent(isA(AfterCreation.class));
	}

	@Data
	static class DummyEntity {
		@Id private final Long Id;
	}
}
