/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;

/**
 * Factory which selects and builds the appropriate {@link InsertStrategy} or {@link BatchInsertStrategy} based on
 * whether the insert is expected to generate ids.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
public class InsertStrategyFactory {
	private final NamedParameterJdbcOperations namedParameterJdbcOperations;
	private final BatchJdbcOperations batchJdbcOperations;
	private final Dialect dialect;

	public InsertStrategyFactory(NamedParameterJdbcOperations namedParameterJdbcOperations, BatchJdbcOperations batchJdbcOperations,
								 Dialect dialect) {
		this.namedParameterJdbcOperations = namedParameterJdbcOperations;
		this.batchJdbcOperations = batchJdbcOperations;
		this.dialect = dialect;
	}

	/**
	 * @param idValueSource the {@link IdValueSource} for the insert.
	 * @param idColumn the identifier for the id, if an id is expected to be generated. May be {@code null}.
	 * @return the {@link InsertStrategy} to be used for the insert.
	 * @since 2.4
	 */
	InsertStrategy insertStrategy(IdValueSource idValueSource, @Nullable SqlIdentifier idColumn) {

		if (IdValueSource.GENERATED.equals(idValueSource)) {
			return new IdGeneratingInsertStrategy(dialect, namedParameterJdbcOperations, idColumn);
		}
		return new DefaultInsertStrategy(namedParameterJdbcOperations);
	}

	/**
	 * @param idValueSource the {@link IdValueSource} for the insert.
	 * @param idColumn the identifier for the id, if an ids are expected to be generated. May be {@code null}.
	 * @return the {@link BatchInsertStrategy} to be used for the batch insert.
	 * @since 2.4
	 */
	BatchInsertStrategy batchInsertStrategy(IdValueSource idValueSource, @Nullable SqlIdentifier idColumn) {

		if (IdValueSource.GENERATED.equals(idValueSource)) {
			return new IdGeneratingBatchInsertStrategy(
					new IdGeneratingInsertStrategy(dialect, namedParameterJdbcOperations, idColumn),
					dialect, batchJdbcOperations, idColumn);
		}
		return new DefaultBatchInsertStrategy(namedParameterJdbcOperations);
	}

	private static class DefaultInsertStrategy implements InsertStrategy {

		private final NamedParameterJdbcOperations jdbcOperations;

		public DefaultInsertStrategy(NamedParameterJdbcOperations jdbcOperations) {
			this.jdbcOperations = jdbcOperations;
		}

		@Override
		public Object execute(String sql, SqlParameterSource sqlParameterSource) {

			jdbcOperations.update(sql, sqlParameterSource);
			return null;
		}
	}

	private static class DefaultBatchInsertStrategy implements BatchInsertStrategy {

		private final NamedParameterJdbcOperations jdbcOperations;

		public DefaultBatchInsertStrategy(NamedParameterJdbcOperations jdbcOperations) {
			this.jdbcOperations = jdbcOperations;
		}

		@Override
		public Object[] execute(String sql, SqlParameterSource[] sqlParameterSources) {

			jdbcOperations.batchUpdate(sql, sqlParameterSources);
			return new Object[sqlParameterSources.length];
		}
	}

}
