/*
 * Copyright 2019-2021 the original author or authors.
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

import org.springframework.data.relational.core.mapping.InsertDefaultValues;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.data.util.Lazy;

/**
 * An SQL dialect for Microsoft SQL Server.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 1.1
 */
public class SqlServerDialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final SqlServerDialect INSTANCE = new SqlServerDialect();

	protected SqlServerDialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getLimit(long)
		 */
		@Override
		public String getLimit(long limit) {
			return "OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY";
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
			return String.format("OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit);
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

	private static final LockClause LOCK_CLAUSE = new LockClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LockClause#getLimit(LockOptions)
		 */
		@Override
		public String getLock(LockOptions lockOptions) {
			switch (lockOptions.getLockMode()) {

				case PESSIMISTIC_WRITE:
					return "WITH (UPDLOCK, ROWLOCK)";

				case PESSIMISTIC_READ:
					return "WITH (HOLDLOCK, ROWLOCK)";

				default:
					return "";
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.AFTER_FROM_TABLE;
		}
	};

	private final Lazy<SelectRenderContext> selectRenderContext = Lazy
			.of(() -> new SqlServerSelectRenderContext(getAfterFromTable(), getAfterOrderBy()));

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
		return LOCK_CLAUSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#getLikeEscaper()
	 */
	@Override
	public Escaper getLikeEscaper() {
		return Escaper.DEFAULT.withRewriteFor("[", "]");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.AbstractDialect#getSelectContext()
	 */
	@Override
	public SelectRenderContext getSelectContext() {
		return selectRenderContext.get();
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.NONE;
	}

	@Override
	public InsertWithDefaultValues getSqlInsertWithDefaultValues() {
		return new InsertWithDefaultValues() {
			@Override
			public String getDefaultInsertPart() {
				return InsertDefaultValues.MS_SQL_SERVER.getDefaultInsertPart();
			}
		};
	}
}
