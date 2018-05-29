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
import org.springframework.data.repository.support.RepositoryInvoker;

/**
 * An {@link AggregateReference} that knows the id of the reference and has the necessary infrastructure to load it
 * using a repository.
 *
 * @param <T>
 * @param <ID>
 */
@RequiredArgsConstructor
public class LazyAggregateReference<T, ID> implements AggregateReference<T, ID> {

	private final ID id;
	@NonNull
	private final RepositoryInvoker repositoryInvoker;

	@Override
	public ID getId() {
		return id;
	}

	@Override
	public T get() {

		return (id == null) ? null : (T) repositoryInvoker.invokeFindById(id).orElse(null);
	}
}
