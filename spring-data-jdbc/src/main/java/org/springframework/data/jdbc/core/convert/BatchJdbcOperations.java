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

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.namedparam.NamedParameterUtils;
import org.springframework.jdbc.core.namedparam.ParsedSql;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Counterpart to {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations} containing
 * methods for performing batch updates with generated keys.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
public class BatchJdbcOperations {
	private final JdbcOperations jdbcOperations;

	public BatchJdbcOperations(JdbcOperations jdbcOperations) {
		this.jdbcOperations = jdbcOperations;
	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments,
	 * returning generated keys.
	 * @param sql the SQL statement to execute
	 * @param batchArgs the array of {@link SqlParameterSource} containing the batch of
	 * arguments for the query
	 * @param generatedKeyHolder a {@link KeyHolder} that will hold the generated keys
	 * @return an array containing the numbers of rows affected by each update in the batch
	 * (may also contain special JDBC-defined negative values for affected rows such as
	 * {@link java.sql.Statement#SUCCESS_NO_INFO}/{@link java.sql.Statement#EXECUTE_FAILED})
	 * @throws org.springframework.dao.DataAccessException if there is any problem issuing the update
	 * @see org.springframework.jdbc.support.GeneratedKeyHolder
	 * @since 2.4
	 */
	int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder) {
		return batchUpdate(sql, batchArgs, generatedKeyHolder, null);
	}

	/**
	 * Execute a batch using the supplied SQL statement with the batch of supplied arguments,
	 * returning generated keys.
	 * @param sql the SQL statement to execute
	 * @param batchArgs the array of {@link SqlParameterSource} containing the batch of
	 * arguments for the query
	 * @param generatedKeyHolder a {@link KeyHolder} that will hold the generated keys
	 * @param keyColumnNames names of the columns that will have keys generated for them
	 * @return an array containing the numbers of rows affected by each update in the batch
	 * (may also contain special JDBC-defined negative values for affected rows such as
	 * {@link java.sql.Statement#SUCCESS_NO_INFO}/{@link java.sql.Statement#EXECUTE_FAILED})
	 * @throws org.springframework.dao.DataAccessException if there is any problem issuing the update
	 * @see org.springframework.jdbc.support.GeneratedKeyHolder
	 * @since 2.4
	 */
	int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder,
			@Nullable String[] keyColumnNames) {

		if (batchArgs.length == 0) {
			return new int[0];
		}
		
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
		SqlParameterSource paramSource = batchArgs[0];
		String sqlToUse = NamedParameterUtils.substituteNamedParameters(parsedSql, paramSource);
		List<SqlParameter> declaredParameters = NamedParameterUtils.buildSqlParameterList(parsedSql, paramSource);
		PreparedStatementCreatorFactory pscf = new PreparedStatementCreatorFactory(sqlToUse, declaredParameters);
		if (keyColumnNames != null) {
			pscf.setGeneratedKeysColumnNames(keyColumnNames);
		} else {
			pscf.setReturnGeneratedKeys(true);
		}
		Object[] params = NamedParameterUtils.buildValueArray(parsedSql, paramSource, null);
		PreparedStatementCreator psc = pscf.newPreparedStatementCreator(params);
		BatchPreparedStatementSetter bpss = new BatchPreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				Object[] values = NamedParameterUtils.buildValueArray(parsedSql, batchArgs[i], null);
				pscf.newPreparedStatementSetter(values).setValues(ps);
			}

			@Override
			public int getBatchSize() {
				return batchArgs.length;
			}
		};
		PreparedStatementCallback<int[]> preparedStatementCallback = ps -> {
			int batchSize = bpss.getBatchSize();
			generatedKeyHolder.getKeyList().clear();
			if (JdbcUtils.supportsBatchUpdates(ps.getConnection())) {
				for (int i = 0; i < batchSize; i++) {
					bpss.setValues(ps, i);
					ps.addBatch();
				}
				int[] results = ps.executeBatch();
				storeGeneratedKeys(generatedKeyHolder, ps, batchSize);
				return results;
			} else {
				List<Integer> rowsAffected = new ArrayList<>();
				for (int i = 0; i < batchSize; i++) {
					bpss.setValues(ps, i);
					rowsAffected.add(ps.executeUpdate());
					storeGeneratedKeys(generatedKeyHolder, ps, 1);
				}
				int[] rowsAffectedArray = new int[rowsAffected.size()];
				for (int i = 0; i < rowsAffectedArray.length; i++) {
					rowsAffectedArray[i] = rowsAffected.get(i);
				}
				return rowsAffectedArray;
			}
		};
		int[] result = jdbcOperations.execute(psc, preparedStatementCallback);
		Assert.state(result != null, "No result array");
		return result;
	}

	private void storeGeneratedKeys(KeyHolder generatedKeyHolder, PreparedStatement ps, int rowsExpected) throws SQLException {

		List<Map<String, Object>> generatedKeys = generatedKeyHolder.getKeyList();
		ResultSet keys = ps.getGeneratedKeys();
		if (keys != null) {
			try {
				RowMapperResultSetExtractor<Map<String, Object>> rse =
						new RowMapperResultSetExtractor<>(new ColumnMapRowMapper(), rowsExpected);
				//noinspection ConstantConditions
				generatedKeys.addAll(rse.extractData(keys));
			}
			finally {
				JdbcUtils.closeResultSet(keys);
			}
		}
	}
}
