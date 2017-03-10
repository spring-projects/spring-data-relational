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

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.mapping.event.AfterCreation;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation;
import org.springframework.jdbc.core.RowMapper;

import lombok.RequiredArgsConstructor;

/**
 * a RowMapper that publishes events after a delegate, did the actual work of mapping a {@link ResultSet} to an entityInformation.
 *
 * @author Jens Schauder
 * @since 2.0
 */
@RequiredArgsConstructor
public class EventPublishingEntityRowMapper<T, ID extends Serializable> implements RowMapper<T> {

	private final RowMapper<T> delegate;
	private final JdbcPersistentEntityInformation<T, ID> entityInformation;
	private final ApplicationEventPublisher publisher;

	@Override
	public T mapRow(ResultSet resultSet, int i) throws SQLException {

		T instance = delegate.mapRow(resultSet, i);

		publisher.publishEvent(new AfterCreation(new Specified(entityInformation.getId(instance)), instance));

		return instance;
	}
}
