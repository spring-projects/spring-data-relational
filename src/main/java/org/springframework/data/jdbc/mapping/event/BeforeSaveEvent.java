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
 * subclasses of this get published before an entity gets saved to the database.
 *
 * @author Jens Schauder
 */
public class BeforeSaveEvent extends JdbcEvent {

	/**
	 * @param instance the entity about to get saved.
	 * @param idProvider a function providing the id, for the instance.
	 * @param <T> type of the entity and the argument of the {@code idProvider}
	 */
	<T> BeforeSaveEvent(T instance, Function<T, Object> idProvider) {
		super(instance, idProvider);
	}
}
