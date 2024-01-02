/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

/**
 * A {@link Dialect} for HsqlDb.
 *
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 */
public class HsqlDbDialect extends AbstractDialect {

	public static final HsqlDbDialect INSTANCE = new HsqlDbDialect();

	protected HsqlDbDialect() {}

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return AnsiDialect.LOCK_CLAUSE;
	}

	@Override
	public boolean supportsSingleQueryLoading() {
		return false;
	}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset;
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return getOffset(offset) + " " + getLimit(limit);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};
}
