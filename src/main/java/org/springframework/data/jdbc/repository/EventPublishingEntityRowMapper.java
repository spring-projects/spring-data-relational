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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.mapping.event.AfterCreationEvent;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.jdbc.core.RowMapper;

/**
 * a RowMapper that publishes events after a delegate, did the actual work of mapping a {@link ResultSet} to an entity.
 *
 * @author Jens Schauder
 */
public class EventPublishingEntityRowMapper<T> implements RowMapper<T> {

	private final RowMapper<T> delegate;
	private final JdbcPersistentEntity<T> entity;
	private final ApplicationEventPublisher publisher;

	/**
	 *
	 * @param delegate does the actuall mapping.
	 * @param entity provides functionality to create ids from entities
	 * @param publisher used for event publishing after the mapping.
	 */
	EventPublishingEntityRowMapper(RowMapper<T> delegate,JdbcPersistentEntity<T> entity, ApplicationEventPublisher publisher) {

		this.delegate = delegate;
		this.entity = entity;
		this.publisher = publisher;
	}

	@Override
	public T mapRow(ResultSet resultSet, int i) throws SQLException {

		T instance = delegate.mapRow(resultSet, i);

		publisher.publishEvent(new AfterCreationEvent(instance, entity::getIdValue));

		return instance;
	}
}
