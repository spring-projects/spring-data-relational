/*
 * Copyright 2026-present the original author or authors.
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
 * Implementation of {@link ReactiveUpsertOperation}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
record ReactiveUpsertOperationSupport(R2dbcEntityTemplate template) implements ReactiveUpsertOperation {

	@Override
	public <T> ReactiveUpsert<T> upsert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");
		return new ReactiveUpsertSupport<>(template, domainType, null);
	}

	static class ReactiveUpsertSupport<T> implements ReactiveUpsert<T> {

		private final R2dbcEntityTemplate template;
		private final Class<T> domainType;
		private final @Nullable SqlIdentifier tableName;

		ReactiveUpsertSupport(R2dbcEntityTemplate template, Class<T> domainType, @Nullable SqlIdentifier tableName) {

			this.template = template;
			this.domainType = domainType;
			this.tableName = tableName;
		}

		@Override
		public TerminatingUpsert<T> inTable(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");
			return new ReactiveUpsertSupport<>(template, domainType, tableName);
		}

		@Override
		public Mono<T> one(T object) {

			Assert.notNull(object, "Object to upsert must not be null");
			return template.doUpsert(object, getTableName());
		}

		private SqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}

	}

}
