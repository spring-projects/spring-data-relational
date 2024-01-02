/*
 * Copyright 2022-2024 the original author or authors.
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

import java.util.Map;
import java.util.Optional;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.IdGeneration;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;

/**
 * An {@link InsertStrategy} that expects an id to be generated from the insert.
 *
 * @author Chirag Tailor
 * @author Kurt Niemi
 * @since 2.4
 */
class IdGeneratingInsertStrategy implements InsertStrategy {

	private final Dialect dialect;
	private final NamedParameterJdbcOperations jdbcOperations;
	private final SqlIdentifier idColumn;

	IdGeneratingInsertStrategy(Dialect dialect, NamedParameterJdbcOperations jdbcOperations,
			@Nullable SqlIdentifier idColumn) {
		this.dialect = dialect;
		this.jdbcOperations = jdbcOperations;
		this.idColumn = idColumn;
	}

	@Override
	public Object execute(String sql, SqlParameterSource sqlParameterSource) {

		KeyHolder holder = new GeneratedKeyHolder();

		IdGeneration idGeneration = dialect.getIdGeneration();

		if (idGeneration.driverRequiresKeyColumnNames()) {

			String[] keyColumnNames = getKeyColumnNames(idGeneration);
			if (keyColumnNames.length == 0) {
				jdbcOperations.update(sql, sqlParameterSource, holder);
			} else {
				jdbcOperations.update(sql, sqlParameterSource, holder, keyColumnNames);
			}
		} else {
			jdbcOperations.update(sql, sqlParameterSource, holder);
		}

		try {
			// MySQL just returns one value with a special name
			return holder.getKey();
		} catch (DataRetrievalFailureException | InvalidDataAccessApiUsageException e) {
			// Postgres returns a value for each column
			// MS SQL Server returns a value that might be null.

			Map<String, Object> keys = holder.getKeys();
			if (keys == null || idColumn == null) {
				return null;
			}

			return keys.get(idColumn.getReference());
		}
	}

	private String[] getKeyColumnNames(IdGeneration idGeneration) {
		return Optional.ofNullable(idColumn).map(idColumn -> new String[] { idGeneration.getKeyColumnName(idColumn) })
				.orElse(new String[0]);
	}
}
