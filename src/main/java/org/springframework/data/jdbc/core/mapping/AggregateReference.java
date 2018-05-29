/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

import lombok.RequiredArgsConstructor;

import org.springframework.lang.Nullable;

/**
 * A reference to another aggregate. It should be assumed that calling {@link #get()} on an {@link AggregateReference}
 * triggers database access.
 *
 * @param <T> the type of the referenced aggregate root.
 * @param <ID> the type of the id of the referenced aggregate root.
 * @author Jens Schauder
 */
public interface AggregateReference<T, ID> {

	static <T, ID> AggregateReference<T, ID> to(ID id) {
		return new IdOnlyAggregateReference<>(id);
	}

	/**
	 * @return the id of the referenced aggregate. May be {@code null}.
	 */
	@Nullable
	ID getId();

	/**
	 * @return the referenced aggregate. May be {@code null} if the id of the reference is {@code null} or when no
	 *         aggregate of type {@code T} is available.
	 */
	@Nullable
	T get();

	/**
	 * An {@link AggregateReference} that only holds the id of the referenced aggregate root. Any attempt to access the
	 * actual referenced aggregate root will throw an {@link UnsupportedOperationException}. Useful when the id to
	 * reference is known but the referenced aggregate root is not required.
	 * 
	 * @param <T>
	 * @param <ID>
	 */
	@RequiredArgsConstructor
	class IdOnlyAggregateReference<T, ID> implements AggregateReference<T, ID> {

		private final ID id;

		@Override
		public ID getId() {
			return id;
		}

		@Override
		public T get() {
			throw new UnsupportedOperationException();
		}
	}

}
