/*
 * Copyright 2017-2021 the original author or authors.
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

import org.springframework.data.relational.core.conversion.AggregateChange;

/**
 * Gets published before an aggregate gets converted into a database change.
 *
 * @since 1.1
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class BeforeConvertEvent<E> extends RelationalEventWithEntity<E> {

	private static final long serialVersionUID = -5716795164911939224L;

	/**
	 * @param instance the saved entity. Must not be {@literal null}.
	 * @since 2.1.4
	 */
	public BeforeConvertEvent(E instance) {
		super(instance);
	}

	/**
	 * @param instance the saved entity. Must not be {@literal null}.
	 * @param change the {@link AggregateChange} encoding the actions to be performed on the database as change. Since
	 *          this event is fired before the conversion the change is actually empty, but contains information if the
	 *          aggregate is considered new in {@link AggregateChange#getKind()}. Must not be {@literal null}.
	 * @deprecated since 2.1.4, use {@link #BeforeConvertEvent(Object)} as we don't expect an {@link AggregateChange}
	 *             before converting an aggregate.
	 */
	@Deprecated
	public BeforeConvertEvent(E instance, AggregateChange<E> change) {
		super(instance);
	}
}
