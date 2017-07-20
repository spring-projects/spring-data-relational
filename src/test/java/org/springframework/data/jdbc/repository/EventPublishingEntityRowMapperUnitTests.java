/*
 * Copyright 2017 the original author or authors.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import lombok.Value;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.EventPublishingEntityRowMapper;
import org.springframework.data.jdbc.mapping.event.AfterCreation;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link EventPublishingEntityRowMapper}.
 * 
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class EventPublishingEntityRowMapperUnitTests {

	@Mock RowMapper<DummyEntity> rowMapperDelegate;
	@Mock JdbcPersistentEntityInformation<DummyEntity, Long> entityInformation;
	@Mock ApplicationEventPublisher publisher;

	@Test // DATAJDBC-99
	public void eventGetsPublishedAfterInstantiation() throws SQLException {

		when(rowMapperDelegate.mapRow(any(ResultSet.class), anyInt())).thenReturn(new DummyEntity(1L));
		when(entityInformation.getRequiredId(any())).thenReturn(1L);

		EventPublishingEntityRowMapper<?> rowMapper = new EventPublishingEntityRowMapper<>(rowMapperDelegate,
				entityInformation, publisher);

		ResultSet resultSet = mock(ResultSet.class);
		rowMapper.mapRow(resultSet, 1);

		verify(publisher).publishEvent(isA(AfterCreation.class));
	}

	@Value
	static class DummyEntity {
		@Id Long Id;
	}
}
