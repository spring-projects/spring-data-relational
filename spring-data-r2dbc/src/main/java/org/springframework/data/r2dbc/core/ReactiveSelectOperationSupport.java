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
package org.springframework.data.r2dbc.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.RowsFetchSpec;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ReactiveSelectOperation}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @since 1.1
 */
class ReactiveSelectOperationSupport implements ReactiveSelectOperation {

	private final R2dbcEntityTemplate template;

	ReactiveSelectOperationSupport(R2dbcEntityTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveSelect<T> select(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveSelectSupport<>(this.template, domainType, domainType, Query.empty(), null, null);
	}

	static class ReactiveSelectSupport<T> implements ReactiveSelect<T> {

		private final R2dbcEntityTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final Query query;
		private final @Nullable SqlIdentifier tableName;
		private final @Nullable Integer fetchSize;

		ReactiveSelectSupport(R2dbcEntityTemplate template, Class<?> domainType, Class<T> returnType, Query query,
				@Nullable SqlIdentifier tableName, @Nullable Integer fetchSize) {

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.tableName = tableName;
			this.fetchSize = fetchSize;
		}

		@Override
		public SelectWithProjection<T> from(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveSelectSupport<>(template, domainType, returnType, query, tableName, fetchSize);
		}

		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ReactiveSelectSupport<>(template, domainType, returnType, query, tableName, fetchSize);
		}

		@Override
		public SelectWithQuery<T> withFetchSize(int fetchSize) {
			return new ReactiveSelectSupport<>(template, domainType, returnType, query, tableName, fetchSize);
		}

		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveSelectSupport<>(template, domainType, returnType, query, tableName, fetchSize);
		}

		@Override
		public Mono<Long> count() {
			return template.doCount(query, domainType, getTableName());
		}

		@Override
		public Mono<Boolean> exists() {
			return template.doExists(query, domainType, getTableName());
		}

		@Override
		public Mono<T> first() {
			return template.doSelect(query.limit(1), domainType, getTableName(), returnType, RowsFetchSpec::first, fetchSize);
		}

		@Override
		public Mono<T> one() {
			return template.doSelect(query.limit(2), domainType, getTableName(), returnType, RowsFetchSpec::one, fetchSize);
		}

		@Override
		public Flux<T> all() {
			return template.doSelect(query, domainType, getTableName(), returnType, RowsFetchSpec::all, fetchSize);
		}

		private SqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}
	}
}
