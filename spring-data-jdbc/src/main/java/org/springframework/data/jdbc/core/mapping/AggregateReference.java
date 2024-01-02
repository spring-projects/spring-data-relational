/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

import java.util.Objects;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A reference to the aggregate root of a different aggregate.
 *
 * @param <T> the type of the referenced aggregate root.
 * @param <ID> the type of the id of the referenced aggregate root.
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 1.0
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
	 * An {@link AggregateReference} that only holds the id of the referenced aggregate root. Note that there is no check
	 * that a matching aggregate for this id actually exists.
	 *
	 * @param <T>
	 * @param <ID>
	 */
	class IdOnlyAggregateReference<T, ID> implements AggregateReference<T, ID> {

		private final ID id;

		public IdOnlyAggregateReference(ID id) {

			Assert.notNull(id, "Id must not be null");

			this.id = id;
		}

		@Override
		public ID getId() {
			return id;
		}

		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			IdOnlyAggregateReference<?, ?> that = (IdOnlyAggregateReference<?, ?>) o;
			return id.equals(that.id);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id);
		}

		@Override
		public String toString() {

			return "IdOnlyAggregateReference{" + "id=" + id + '}';
		}
	}
}
