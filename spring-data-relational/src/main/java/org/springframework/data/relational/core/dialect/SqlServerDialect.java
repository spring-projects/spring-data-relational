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

	private static final IdGeneration ID_GENERATION = new IdGeneration() {

		@Override
		public boolean supportedForBatchOperations() {
			return false;
		}
	};

	private static final IdentifierProcessing IDENTIFIER_PROCESSING = IdentifierProcessing
			.create(IdentifierProcessing.Quoting.ANSI, IdentifierProcessing.LetterCasing.AS_IS);

	protected SqlServerDialect() {}

	@Override
	public IdGeneration getIdGeneration() {
		return ID_GENERATION;
	}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return "OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY";
		}

		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset + " ROWS";
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private static final LockClause LOCK_CLAUSE = new LockClause() {

		@Override
		public String getLock(LockOptions lockOptions) {
			
			return switch (lockOptions.getLockMode()) {
				case PESSIMISTIC_WRITE -> "WITH (UPDLOCK, ROWLOCK)";
				case PESSIMISTIC_READ -> "WITH (HOLDLOCK, ROWLOCK)";
			};
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_FROM_TABLE;
		}
	};

	private final Lazy<SelectRenderContext> selectRenderContext = Lazy
			.of(() -> new SqlServerSelectRenderContext(getAfterFromTable(), getAfterOrderBy()));

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	@Override
	public Escaper getLikeEscaper() {
		return Escaper.DEFAULT.withRewriteFor("[", "]");
	}

	@Override
	public SelectRenderContext getSelectContext() {
		return selectRenderContext.get();
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IDENTIFIER_PROCESSING;
	}

	@Override
	public InsertRenderContext getInsertRenderContext() {
		return InsertRenderContexts.MS_SQL_SERVER;
	}

	@Override
	public OrderByNullPrecedence orderByNullHandling() {
		return OrderByNullPrecedence.NONE;
	}
}
