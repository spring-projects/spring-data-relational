/*
 * Copyright 2020-2024 the original author or authors.
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

import org.springframework.context.ApplicationEvent;

/**
 * Base class for mapping events of Spring Data Relational
 *
 * @param <E> the type this event refers to.
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
public abstract class AbstractRelationalEvent<E> extends ApplicationEvent implements RelationalEvent<E> {

	/**
	 * Creates an event with the given source. The source might be an entity or an id of an entity, depending on the
	 * actual event subclass.
	 *
	 * @param source must not be {@literal null}.
	 */
	public AbstractRelationalEvent(Object source) {
		super(source);
	}
}
