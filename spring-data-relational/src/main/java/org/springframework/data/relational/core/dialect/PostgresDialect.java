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

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.util.Assert;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Jens Schauder
 * @author Nikita Konev
 * @author Mikhail Polivakha
 * @since 1.1
 */
public class PostgresDialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 *
	 * @deprecated use either the {@code org.springframework.data.r2dbc.dialect.PostgresDialect} or
	 * 						 {@code org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect}.
	 */
	@Deprecated(forRemoval = true)
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	private static final Set<Class<?>> POSTGRES_SIMPLE_TYPES = Set.of(UUID.class, URL.class, URI.class, InetAddress.class,
			Map.class);

	private static final IdentifierProcessing identifierProcessing = IdentifierProcessing.create(Quoting.ANSI,
			LetterCasing.LOWER_CASE);

	private IdGeneration idGeneration = new IdGeneration() {

		@Override
		public String createSequenceQuery(SqlIdentifier sequenceName) {
			return "SELECT nextval('%s')".formatted(sequenceName.toSql(getIdentifierProcessing()));
		}
	};

	protected PostgresDialect() {}

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
			return String.format("LIMIT %d OFFSET %d", limit, offset);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private static final ObjectArrayColumns ARRAY_COLUMNS = ObjectArrayColumns.INSTANCE;

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	private final PostgresLockClause LOCK_CLAUSE = new PostgresLockClause();

	@Override
	public LockClause lock() {
		return LOCK_CLAUSE;
	}

	@Override
	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	@Override
	public Collection<Object> getConverters() {
		return Collections.singletonList(TimestampAtUtcToOffsetDateTimeConverter.INSTANCE);
	}

	static class PostgresLockClause implements LockClause {

		@Override
		public String getLock(LockOptions lockOptions) {

			List<TableLike> tables = lockOptions.getFrom().getTables();
			if (tables.isEmpty()) {
				return "";
			}

			// get the first table and obtain last part if the identifier is a composed one.
			SqlIdentifier identifier = tables.get(0).getName();
			SqlIdentifier last = identifier;

			for (SqlIdentifier sqlIdentifier : identifier) {
				last = sqlIdentifier;
			}

			// without schema
			String tableName = last.toSql(PostgresDialect.identifierProcessing);

			return switch (lockOptions.getLockMode()) {
				case PESSIMISTIC_WRITE -> "FOR UPDATE OF " + tableName;
				case PESSIMISTIC_READ -> "FOR SHARE OF " + tableName;
			};
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return identifierProcessing;
	}

	@Override
	public Set<Class<?>> simpleTypes() {
		return POSTGRES_SIMPLE_TYPES;
	}

	@Override
	public SimpleFunction getExistsFunction() {
		return Functions.least(Functions.count(SQL.literalOf(1)), SQL.literalOf(1));
	}

	@Override
	public IdGeneration getIdGeneration() {
		return idGeneration;
	}
}
