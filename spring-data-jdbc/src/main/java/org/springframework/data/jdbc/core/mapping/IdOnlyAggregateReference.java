/*
 * Copyright 2025 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * An {@link AggregateReference} that only holds the id of the referenced aggregate root. Note that there is no check
 * that a matching aggregate for this id actually exists.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 4.0
 * @param <T>
 * @param <ID>
 */
record IdOnlyAggregateReference<T, ID>(ID id) implements AggregateReference<T, ID> {

	IdOnlyAggregateReference {
		Assert.notNull(id, "Id must not be null");
	}

	@Override
	public ID getId() {
		return id();
	}

	@Override
	public String toString() {
		return "IdOnlyAggregateReference{" + "id=" + id + '}';
	}
}
