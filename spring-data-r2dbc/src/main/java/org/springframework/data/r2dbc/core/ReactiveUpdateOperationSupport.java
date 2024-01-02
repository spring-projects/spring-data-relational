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

import reactor.core.publisher.Mono;

import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ReactiveUpdateOperation}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ReactiveUpdateOperationSupport implements ReactiveUpdateOperation {

	private final R2dbcEntityTemplate template;

	ReactiveUpdateOperationSupport(R2dbcEntityTemplate template) {
		this.template = template;
	}

	@Override
	public ReactiveUpdate update(Class<?> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveUpdateSupport(template, domainType, Query.empty(), null);
	}

	static class ReactiveUpdateSupport implements ReactiveUpdate, TerminatingUpdate {

		private final R2dbcEntityTemplate template;
		private final Class<?> domainType;
		private final Query query;
		private final @Nullable SqlIdentifier tableName;

		ReactiveUpdateSupport(R2dbcEntityTemplate template, Class<?> domainType, Query query,
				@Nullable SqlIdentifier tableName) {

			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.tableName = tableName;
		}

		@Override
		public UpdateWithQuery inTable(SqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveUpdateSupport(template, domainType, query, tableName);
		}

		@Override
		public TerminatingUpdate matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveUpdateSupport(template, domainType, query, tableName);
		}

		@Override
		public Mono<Long> apply(Update update) {

			Assert.notNull(update, "Update must not be null");

			return template.doUpdate(query, update, domainType, getTableName());
		}

		private SqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(this.domainType);
		}
	}
}
