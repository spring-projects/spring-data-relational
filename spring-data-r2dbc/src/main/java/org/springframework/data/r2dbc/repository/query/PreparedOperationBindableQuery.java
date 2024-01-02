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
package org.springframework.data.r2dbc.repository.query;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.util.Assert;

/**
 * A {@link BindableQuery} implementation based on a {@link PreparedOperation}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Will Easterling
 */
class PreparedOperationBindableQuery implements BindableQuery {

	private final PreparedOperation<?> preparedQuery;

	/**
	 * Creates new instance of this class with the given {@link PreparedOperation}.
	 *
	 * @param preparedQuery prepared SQL query, must not be {@literal null}.
	 */
	PreparedOperationBindableQuery(PreparedOperation<?> preparedQuery) {

		Assert.notNull(preparedQuery, "Prepared query must not be null");

		this.preparedQuery = preparedQuery;
	}

	@Override
	public DatabaseClient.GenericExecuteSpec bind(DatabaseClient.GenericExecuteSpec bindSpec) {
		BindSpecBindTargetAdapter bindTargetAdapter = new BindSpecBindTargetAdapter(bindSpec);
		preparedQuery.bindTo(bindTargetAdapter);
		return bindTargetAdapter.bindSpec;
	}

	@Override
	public String get() {
		return preparedQuery.get();
	}

	/**
	 * This class adapts {@link DatabaseClient.GenericExecuteSpec} to {@link BindTarget} allowing easy binding of query
	 * parameters using {@link PreparedOperation}.
	 */
	static class BindSpecBindTargetAdapter implements BindTarget {

		DatabaseClient.GenericExecuteSpec bindSpec;

		private BindSpecBindTargetAdapter(DatabaseClient.GenericExecuteSpec bindSpec) {
			this.bindSpec = bindSpec;
		}

		@Override
		public void bind(String identifier, Object value) {
			this.bindSpec = this.bindSpec.bind(identifier, value);
		}

		@Override
		public void bind(int index, Object value) {
			this.bindSpec = this.bindSpec.bind(index, value);
		}

		@Override
		public void bindNull(String identifier, Class<?> type) {
			this.bindSpec = this.bindSpec.bindNull(identifier, type);
		}

		@Override
		public void bindNull(int index, Class<?> type) {
			this.bindSpec = this.bindSpec.bindNull(index, type);
		}
	}
}
