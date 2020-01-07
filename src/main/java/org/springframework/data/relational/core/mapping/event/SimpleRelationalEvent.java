/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.mapping.event;

import java.util.Optional;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.lang.Nullable;

/**
 * The common superclass for all events published by JDBC repositories. {@link #getSource} contains the
 * {@link Identifier} of the entity triggering the event.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
class SimpleRelationalEvent extends ApplicationEvent implements RelationalEvent {

	private static final long serialVersionUID = -1798807778668751659L;

	private final Object entity;
	private final AggregateChange change;

	SimpleRelationalEvent(Identifier id, Optional<Object> entity, @Nullable AggregateChange change) {

		super(id);

		this.entity = entity.orElse(null);
		this.change = change;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.mapping.event.JdbcEvent#getId()
	 */
	@Override
	public Identifier getId() {
		return (Identifier) getSource();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.mapping.event.JdbcEvent#getOptionalEntity()
	 */
	@Override
	public Optional<Object> getOptionalEntity() {
		return Optional.ofNullable(entity);
	}

	public AggregateChange getChange() {
		return change;
	}
}
