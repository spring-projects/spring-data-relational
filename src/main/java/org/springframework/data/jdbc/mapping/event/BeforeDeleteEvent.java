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
 * gets published when an entity is about to get deleted. {@link ApplicationEvent#getSource()} might contain either the
 * entity or the id of the entity, depending on which delete method was used.
 *
 * @author Jens Schauder
 */
public class BeforeDeleteEvent extends JdbcEvent {

	/**
	 * @param instance the entity about to get deleted. Might be {@literal NULL}
	 * @param idProvider a function providing the id, for the instance. Must provide a not {@literal NULL} id, when called with {@link #instance}
	 * @param <T> type of the entity and the argument of the {@code idProvider}
	 */
	public <T> BeforeDeleteEvent(T instance, Function<T, Object> idProvider) {
		super(instance, idProvider);
	}
}
