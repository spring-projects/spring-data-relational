/*
 * Copyright 2019 the original author or authors.
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

import lombok.RequiredArgsConstructor;

import org.springframework.data.relational.domain.IdentifierProcessing;
import org.springframework.data.relational.domain.IdentifierProcessing.DefaultIdentifierProcessing;
import org.springframework.data.relational.domain.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.domain.IdentifierProcessing.Quoting;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class PostgresDialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	protected PostgresDialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getLimit(long)
		 */
		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getOffset(long)
		 */
		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("LIMIT %d OFFSET %d", limit, offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private final PostgresArrayColumns ARRAY_COLUMNS = new PostgresArrayColumns();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#limit()
	 */
	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	@RequiredArgsConstructor
	static class PostgresArrayColumns implements ArrayColumns {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.ArrayColumns#isSupported()
		 */
		@Override
		public boolean isSupported() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.ArrayColumns#getArrayType(java.lang.Class)
		 */
		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Assert.notNull(userType, "Array component type must not be null");

			return ClassUtils.resolvePrimitiveIfNecessary(userType);
		}
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return new DefaultIdentifierProcessing(Quoting.ANSI, LetterCasing.LOWER_CASE);
	}
}
