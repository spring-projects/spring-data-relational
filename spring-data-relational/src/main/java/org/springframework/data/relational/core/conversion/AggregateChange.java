/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import java.util.function.Consumer;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Chirag Tailor
 */
public interface AggregateChange<T> {

	/**
	 * Returns the {@link Kind} of {@code AggregateChange} this is.
	 *
	 * @return guaranteed to be not {@literal null}.
	 */
	Kind getKind();

	/**
	 * The type of the root of this {@code AggregateChange}.
	 *
	 * @return Guaranteed to be not {@literal null}.
	 */
	Class<T> getEntityType();

	/**
	 * Applies the given consumer to each {@link DbAction} in this {@code AggregateChange}.
	 *
	 * @param consumer must not be {@literal null}.
	 */
	void forEachAction(Consumer<? super DbAction<?>> consumer);

	/**
	 * The kind of action to be performed on an aggregate.
	 */
	enum Kind {

		/**
		 * A {@code SAVE} of an aggregate typically involves an {@code insert} or {@code update} on the aggregate root plus
		 * {@code insert}s, {@code update}s, and {@code delete}s on the other elements of an aggregate.
		 */
		SAVE,

		/**
		 * A {@code DELETE} of an aggregate typically involves a {@code delete} on all contained entities.
		 */
		DELETE
	}
}
