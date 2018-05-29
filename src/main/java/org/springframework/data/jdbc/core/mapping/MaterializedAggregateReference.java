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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * References an aggregate that is already available. The {@link JdbcMappingContext} is necessary in order to
 * determine the id of the aggregate root. A change of the id in the aggregate root is represented in the result of
 * any later calls to {@link #getId()}.
 *
 * @param <T>
 * @param <ID>
 */
@RequiredArgsConstructor
public class MaterializedAggregateReference<T, ID> implements AggregateReference<T, ID> {

	@NonNull
	private final T aggregateRoot;
	@NonNull private final JdbcMappingContext context;

	@SuppressWarnings("unchecked")
	@Override
	public ID getId() {

		return (ID) context //
				.getRequiredPersistentEntity(aggregateRoot.getClass()) //
				.getIdentifierAccessor(aggregateRoot) //
				.getRequiredIdentifier();
	}

	@Override
	public T get() {
		return aggregateRoot;
	}
}
