/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.r2dbc.core;

import reactor.core.publisher.Mono;

import org.jspecify.annotations.Nullable;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ReactiveInsertOperation}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
record ReactiveInsertOperationSupport(R2dbcEntityTemplate template) implements ReactiveInsertOperation {

	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveInsertSupport<>(template, domainType, null);
	}

	static class ReactiveInsertSupport<T> implements ReactiveInsert<T> {

		private final R2dbcEntityTemplate template;
		private final Class<T> domainType;
		private final @Nullable SqlIdentifier tableName;

		ReactiveInsertSupport(R2dbcEntityTemplate template, Class<T> domainType, @Nullable SqlIdentifier tableName) {

			this.template = template;
			this.domainType = domainType;
			this.tableName = tableName;
		}

		@Override
		public TerminatingInsert<T> into(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveInsertSupport<>(template, domainType, tableName);
		}

		@Override
		public Mono<T> using(T object) {

			Assert.notNull(object, "Object to insert must not be null");

			return template.doInsert(object, getTableName());
		}

		private SqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}
	}
}
