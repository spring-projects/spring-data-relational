/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * A SQL dialect for MySQL.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 1.1
 */
public class MySqlDialect extends AbstractDialect {

	/**
	 * MySQL defaults for {@link IdentifierProcessing}.
	 * 
	 * @deprecated Construct your own {@link IdentifierProcessing}. There is no one standard identifier processing for
	 *             MySql.See
	 * 
	 *             <pre>
	 * 	  <a href=
	"https://dev.mysql.com/doc/refman/8.4/en/identifier-case-sensitivity.html">Identifier Case Sensitivity</a>
	 *             </pre>
	 */
	@Deprecated(forRemoval = true,
			since = "4.0") public static final IdentifierProcessing MYSQL_IDENTIFIER_PROCESSING = IdentifierProcessing
					.create(new Quoting("`"), LetterCasing.LOWER_CASE);

	/**
	 * Singleton instance.
	 *
	 * @deprecated use either the {@code org.springframework.data.r2dbc.dialect.MySqlDialect} or
	 *             {@code org.springframework.data.jdbc.core.dialect.JdbcMySqlDialect}
	 */
	@Deprecated(forRemoval = true) public static final MySqlDialect INSTANCE = new MySqlDialect();

	private final IdentifierProcessing identifierProcessing;

	protected MySqlDialect() {
		this(MYSQL_IDENTIFIER_PROCESSING);
	}

	/**
	 * Creates a new {@link MySqlDialect} given {@link IdentifierProcessing}.
	 *
	 * @param identifierProcessing must not be null.
	 * @since 2.0
	 */
	public MySqlDialect(IdentifierProcessing identifierProcessing) {

		Assert.notNull(identifierProcessing, "IdentifierProcessing must not be null");

		this.identifierProcessing = identifierProcessing;
	}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		@Override
		public String getOffset(long offset) {
			// Ugly but the official workaround for offset without limit
			// see: https://stackoverflow.com/a/271650
			return String.format("LIMIT %d, 18446744073709551615", offset);
		}

		@Override
		public String getLimitOffset(long limit, long offset) {

			// LIMIT {[offset,] row_count}
			return String.format("LIMIT %s, %s", offset, limit);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private static final LockClause LOCK_CLAUSE = new LockClause() {

		@Override
		public String getLock(LockOptions lockOptions) {
			switch (lockOptions.getLockMode()) {

				case PESSIMISTIC_WRITE:
					return "FOR UPDATE";

				case PESSIMISTIC_READ:
					return "LOCK IN SHARE MODE";

				default:
					return "";
			}
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return identifierProcessing;
	}

	@Override
	public Collection<Object> getConverters() {
		return Arrays.asList(TimestampAtUtcToOffsetDateTimeConverter.INSTANCE, NumberToBooleanConverter.INSTANCE);
	}

	@Override
	public OrderByNullPrecedence orderByNullHandling() {
		return OrderByNullPrecedence.NONE;
	}

	@Override
	public IdGeneration getIdGeneration() {

		return new IdGeneration() {

			@Override
			public boolean sequencesSupported() {
				return false;
			}

			@Override
			public String createSequenceQuery(SqlIdentifier sequenceName) {
				throw new UnsupportedOperationException(
						"Currently, there is no support for sequence generation for %s dialect. If you need it, please, submit a ticket"
								.formatted(this.getClass().getSimpleName()));
			}
		};
	}
}
