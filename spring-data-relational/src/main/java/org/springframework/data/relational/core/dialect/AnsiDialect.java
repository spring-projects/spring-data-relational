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

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockOptions;

/**
 * An SQL dialect for the ANSI SQL standard.
 *
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @since 2.0
 */
public class AnsiDialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final AnsiDialect INSTANCE = new AnsiDialect();

	protected AnsiDialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return String.format("FETCH FIRST %d ROWS ONLY", limit);
		}

		@Override
		public String getOffset(long offset) {
			return String.format("OFFSET %d ROWS", offset);
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("OFFSET %d ROWS FETCH FIRST %d ROWS ONLY", offset, limit);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	static final LockClause LOCK_CLAUSE = new LockClause() {

		@Override
		public String getLock(LockOptions lockOptions) {
			return "FOR UPDATE";
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private final ArrayColumns ARRAY_COLUMNS = ObjectArrayColumns.INSTANCE;

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	@Override
	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.ANSI;
	}
}
