/*
 * Copyright 2018-2020 the original author or authors.
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

import org.springframework.data.r2dbc.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ReactiveDeleteOperation}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ReactiveDeleteOperationSupport implements ReactiveDeleteOperation {

	private final R2dbcEntityTemplate template;

	ReactiveDeleteOperationSupport(R2dbcEntityTemplate template) {
		this.template = template;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveDeleteOperation#delete(java.lang.Class)
	 */
	@Override
	public ReactiveDelete delete(Class<?> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveDeleteSupport(this.template, domainType, Query.empty(), null);
	}

	static class ReactiveDeleteSupport implements ReactiveDelete, TerminatingDelete {

		private final R2dbcEntityTemplate template;

		private final Class<?> domainType;

		private final Query query;

		private final @Nullable String tableName;

		ReactiveDeleteSupport(R2dbcEntityTemplate template, Class<?> domainType, Query query, @Nullable String tableName) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.tableName = tableName;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.core.ReactiveDeleteOperation.DeleteWithTable#from(java.lang.String)
		 */
		@Override
		public DeleteWithQuery from(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty");

			return new ReactiveDeleteSupport(this.template, this.domainType, this.query, tableName);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.core.ReactiveDeleteOperation.DeleteWithQuery#matching(org.springframework.data.r2dbc.query.Query)
		 */
		@Override
		public TerminatingDelete matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveDeleteSupport(this.template, this.domainType, query, this.tableName);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.core.ReactiveDeleteOperation.TerminatingDelete#all()
		 */
		public Mono<Integer> all() {
			return this.template.doDelete(this.query, this.domainType, getTableName());
		}

		private String getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}
	}
}
