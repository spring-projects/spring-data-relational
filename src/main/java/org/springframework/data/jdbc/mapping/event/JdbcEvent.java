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
package org.springframework.data.jdbc.mapping.event;

import java.util.Optional;

import org.springframework.context.ApplicationEvent;

import lombok.Getter;

/**
 * The common superclass for all events published by JDBC repositories.
 * {@link #getSource} contains the {@link Identifier} of the entity triggering the event.
 *
 * @author Jens Schauder
 * @since 2.0
 */
@Getter
public class JdbcEvent extends ApplicationEvent {

	/**
	 * The optional entity for which this event was published. Might be empty in cases of delete events where only the identifier
	 * was provided to the delete method.
	 *
	 * @return The entity triggering this event or empty.
	 */
	private final Optional<Object> optionalEntity;

	public JdbcEvent(Identifier id, Optional<Object> optionalEntity) {
		super(id);
		this.optionalEntity = optionalEntity;
	}

	/**
	 * The identifier of the entity, triggering this event. Also available via
	 * {@link #getSource()}.
	 *
	 * @return
	 */
	public Identifier getId() {
		return (Identifier) getSource();
	}

}
