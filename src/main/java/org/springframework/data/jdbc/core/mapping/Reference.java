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
 * @author Jens Schauder
 */
public interface Reference<T, ID> {

	static <T, ID> Reference<T, ID> to(JdbcMappingContext context, T aggregateRoot) {
		return new MaterializedReference<T, ID>(context, aggregateRoot);
	}

	static <T, ID> Reference<T, ID> to(ID id) {
		return new IdOnlyReference<>(id);
	}

	static <T, ID> Reference<T, ID> to(ID id, RepositoryInvoker repositoryInvoker) {
		return new LazyReference<>(id, repositoryInvoker);
	}

	ID getId();

	T get();

	@RequiredArgsConstructor
	class MaterializedReference<T, ID> implements Reference<T, ID> {

		@NonNull private final JdbcMappingContext context;
		@NonNull private final T aggregateRoot;

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

	@RequiredArgsConstructor
	class IdOnlyReference<T, ID> implements Reference<T, ID> {

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

	@RequiredArgsConstructor
	class LazyReference<T, ID> implements Reference<T, ID> {

		private final ID id;
		@NonNull private final RepositoryInvoker repositoryInvoker;

		@Override
		public ID getId() {
			return id;
		}

		@Override
		public T get() {

			return (id == null) ? null : (T) repositoryInvoker.invokeFindById(id).orElse(null);
		}
	}
}
