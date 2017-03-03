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

import java.util.function.Function;

import org.springframework.context.ApplicationEvent;

/**
 * is the common superclass for all events published by JDBC repositories.
 *
 * It is recommendet not to use the {@link #getSource()} since it may contain the entity if it was available, when the
 * event was published, or in case of delete events only the Id.
 *
 * Use the dedicated methods {@link #getId()} or {@link #getInstance()} instead. Note that the later might be
 * {@literal NULL} in the cases mentioned above.
 *
 * @author Jens Schauder
 */
public class JdbcEvent extends ApplicationEvent {

	private final Object id;
	private final Object instance;

	<T> JdbcEvent(T instance, Function<T, Object> idProvider) {

		super(instance == null ? idProvider.apply(instance) : instance);
		this.instance = instance;
		this.id = idProvider.apply(instance);
	}

	/**
	 * the entity for which this event was publish. Might be {@literal NULL} in cases of delete events where only the id
	 * was provided to the delete method.
	 *
	 * @return instance of the entity triggering this event.
	 */
	public Object getInstance() {
		return instance;
	}

	/**
	 * the id of the entity, triggering this event. Guaranteed not to be {@literal NULL}.
	 * @return
	 */
	public Object getId() {
		return id;
	}
}
