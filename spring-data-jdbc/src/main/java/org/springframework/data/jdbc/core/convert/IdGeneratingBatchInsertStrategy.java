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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.IdGeneration;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.lang.Nullable;

/**
 * A {@link BatchInsertStrategy} that expects ids to be generated from the batch insert. When the {@link Dialect} does
 * not support id generation for batch operations, this implementation falls back to performing the inserts serially.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
class IdGeneratingBatchInsertStrategy implements BatchInsertStrategy {

	private final InsertStrategy insertStrategy;
	private final Dialect dialect;
	private final BatchJdbcOperations batchJdbcOperations;
	private final SqlIdentifier idColumn;

	public IdGeneratingBatchInsertStrategy(InsertStrategy insertStrategy,
										   Dialect dialect, BatchJdbcOperations batchJdbcOperations,
										   @Nullable SqlIdentifier idColumn) {
		this.insertStrategy = insertStrategy;
		this.dialect = dialect;
		this.batchJdbcOperations = batchJdbcOperations;
		this.idColumn = idColumn;
	}

	@Override
	public Object[] execute(String sql, SqlParameterSource[] sqlParameterSources) {

		if (!dialect.getIdGeneration().supportedForBatchOperations()) {
			return Arrays.stream(sqlParameterSources)
					.map(sqlParameterSource -> insertStrategy.execute(sql, sqlParameterSource))
					.toArray();
		}

		GeneratedKeyHolder holder = new GeneratedKeyHolder();
		IdGeneration idGeneration = dialect.getIdGeneration();
		if (idGeneration.driverRequiresKeyColumnNames()) {

			String[] keyColumnNames = getKeyColumnNames();
			if (keyColumnNames.length == 0) {
				batchJdbcOperations.batchUpdate(sql, sqlParameterSources, holder);
			} else {
				batchJdbcOperations.batchUpdate(sql, sqlParameterSources, holder, keyColumnNames);
			}
		} else {
			batchJdbcOperations.batchUpdate(sql, sqlParameterSources, holder);
		}
		Object[] ids = new Object[sqlParameterSources.length];
		List<Map<String, Object>> keyList = holder.getKeyList();
		for (int i = 0; i < keyList.size(); i++) {
			Map<String, Object> keys = keyList.get(i);
			if (keys.size() > 1) {
				if (idColumn != null) {
					ids[i] = keys.get(idColumn.getReference(dialect.getIdentifierProcessing()));
				}
			} else {
				ids[i] = keys.entrySet().stream().findFirst() //
						.map(Map.Entry::getValue) //
						.orElseThrow(() -> new IllegalStateException("KeyHolder contains an empty key list."));
			}
		}
		return ids;
	}

	private String[] getKeyColumnNames() {

		return Optional.ofNullable(idColumn)
				.map(idColumn -> new String[]{idColumn.getReference(dialect.getIdentifierProcessing())})
				.orElse(new String[0]);
	}
}
