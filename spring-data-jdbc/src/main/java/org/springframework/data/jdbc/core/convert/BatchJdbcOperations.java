/*
 * Copyright 2022-2023 the original author or authors.
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

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;

/**
 * Counterpart to {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations} containing methods for
 * performing batch updates with generated keys.
 *
 * @author Chirag Tailor
 * @author Mark Paluch
 * @since 2.4
 * @deprecated since 3.2. Use {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations#batchUpdate}
 *             methods instead.
 */
@Deprecated(since = "3.2")
public class BatchJdbcOperations {

	private final NamedParameterJdbcOperations jdbcOperations;

	public BatchJdbcOperations(JdbcOperations jdbcOperations) {
		this.jdbcOperations = new NamedParameterJdbcTemplate(jdbcOperations);
	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments, returning generated keys.
	 *
	 * @param sql the SQL statement to execute
	 * @param batchArgs the array of {@link SqlParameterSource} containing the batch of arguments for the query
	 * @param generatedKeyHolder a {@link KeyHolder} that will hold the generated keys
	 * @return an array containing the numbers of rows affected by each update in the batch (may also contain special
	 *         JDBC-defined negative values for affected rows such as
	 *         {@link java.sql.Statement#SUCCESS_NO_INFO}/{@link java.sql.Statement#EXECUTE_FAILED})
	 * @throws org.springframework.dao.DataAccessException if there is any problem issuing the update
	 * @see org.springframework.jdbc.support.GeneratedKeyHolder
	 * @since 2.4
	 */
	int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder) {
		return jdbcOperations.batchUpdate(sql, batchArgs, generatedKeyHolder);
	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments, returning generated keys.
	 *
	 * @param sql the SQL statement to execute
	 * @param batchArgs the array of {@link SqlParameterSource} containing the batch of arguments for the query
	 * @param generatedKeyHolder a {@link KeyHolder} that will hold the generated keys
	 * @param keyColumnNames names of the columns that will have keys generated for them
	 * @return an array containing the numbers of rows affected by each update in the batch (may also contain special
	 *         JDBC-defined negative values for affected rows such as
	 *         {@link java.sql.Statement#SUCCESS_NO_INFO}/{@link java.sql.Statement#EXECUTE_FAILED})
	 * @throws org.springframework.dao.DataAccessException if there is any problem issuing the update
	 * @see org.springframework.jdbc.support.GeneratedKeyHolder
	 * @since 2.4
	 */
	int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder,
			@Nullable String[] keyColumnNames) {
		return jdbcOperations.batchUpdate(sql, batchArgs, generatedKeyHolder, keyColumnNames);
	}
}
