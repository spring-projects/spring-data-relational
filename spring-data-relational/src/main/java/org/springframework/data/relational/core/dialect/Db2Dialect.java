/*
 * Copyright 2020-2022 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockOptions;

/**
 * An SQL dialect for DB2.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class Db2Dialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final Db2Dialect INSTANCE = new Db2Dialect();

	protected Db2Dialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getLimit(long)
		 */
		@Override
		public String getLimit(long limit) {
			return "FETCH FIRST " + limit + " ROWS ONLY";
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getOffset(long)
		 */
		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset + " ROWS";
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("OFFSET %d ROWS FETCH FIRST %d ROWS ONLY", offset, limit);
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
	 * @see org.springframework.data.relational.core.dialect.Dialect#lock()
	 */
	@Override
	public LockClause lock() {

		return new LockClause() {

			@Override
			public String getLock(LockOptions lockOptions) {
				return "FOR UPDATE WITH RS USE AND KEEP EXCLUSIVE LOCKS";
			}

			@Override
			public Position getClausePosition() {
				return Position.AFTER_ORDER_BY;
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#getIdentifierProcessing()
	 */
	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.ANSI;
	}

	@Override
	public Collection<Object> getConverters() {
		return Collections.singletonList(TimestampAtUtcToOffsetDateTimeConverter.INSTANCE);
	}
}
