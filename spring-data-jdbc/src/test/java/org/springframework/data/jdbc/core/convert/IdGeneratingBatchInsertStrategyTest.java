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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.IdGeneration;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Unit tests for {@link IdGeneratingBatchInsertStrategy}.
 *
 * @author Chirag Tailor
 */
class IdGeneratingBatchInsertStrategyTest {

	SqlIdentifier idColumn = SqlIdentifier.quoted("id");
	IdentifierProcessing identifierProcessing = IdentifierProcessing.ANSI;
	NamedParameterJdbcOperations jdbcOperations = mock(NamedParameterJdbcOperations.class);
	InsertStrategy insertStrategy = mock(InsertStrategy.class);
	String sql = "some sql";
	SqlParameterSource[] sqlParameterSources = new SqlParameterSource[] { new SqlIdentifierParameterSource() };

	@Test
	void insertsSequentially_whenIdGenerationForBatchOperationsNotSupported() {

		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, true, false), jdbcOperations, idColumn);

		SqlIdentifierParameterSource sqlParameterSource1 = new SqlIdentifierParameterSource();
		sqlParameterSource1.addValue(SqlIdentifier.quoted("property1"), "value1");
		SqlIdentifierParameterSource sqlParameterSource2 = new SqlIdentifierParameterSource();
		sqlParameterSource2.addValue(SqlIdentifier.quoted("property2"), "value2");

		long id1 = 1L;
		when(insertStrategy.execute(sql, sqlParameterSource1)).thenReturn(id1);
		long id2 = 2L;
		when(insertStrategy.execute(sql, sqlParameterSource2)).thenReturn(id2);

		Object[] ids = batchInsertStrategy.execute(sql,
				new SqlParameterSource[] { sqlParameterSource1, sqlParameterSource2 });

		assertThat(ids).containsExactly(id1, id2);
	}

	@Test
	void insertsWithKeyHolderAndKeyColumnNames_whenDriverRequiresKeyColumnNames() {

		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, true, true), jdbcOperations, idColumn);

		batchInsertStrategy.execute(sql, sqlParameterSources);

		verify(jdbcOperations).batchUpdate(eq(sql), eq(sqlParameterSources), any(KeyHolder.class),
				eq(new String[] { idColumn.getReference() }));
	}

	@Test
	void insertsWithKeyHolder_whenDriverRequiresKeyColumnNames_butIdColumnIsNull() {

		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, true, true), jdbcOperations, null);

		batchInsertStrategy.execute(sql, sqlParameterSources);

		verify(jdbcOperations).batchUpdate(eq(sql), eq(sqlParameterSources), any(KeyHolder.class));
	}

	@Test
	void insertsWithKeyHolder_whenDriverDoesNotRequireKeyColumnNames() {

		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, false, true), jdbcOperations, idColumn);

		batchInsertStrategy.execute(sql, sqlParameterSources);

		verify(jdbcOperations).batchUpdate(eq(sql), eq(sqlParameterSources), any(KeyHolder.class));
	}

	@Test
	void insertsWithKeyHolder_returningKey_whenThereIsOnlyOne() {

		Long idValue = 123L;
		when(jdbcOperations.batchUpdate(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put("anything", idValue);
			keyHolder.getKeyList().add(keys);
			return null;
		});
		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, false, true), jdbcOperations, idColumn);

		Object[] ids = batchInsertStrategy.execute(sql, sqlParameterSources);

		assertThat(ids).containsExactly(idValue);
	}

	@Test
	void insertsWithKeyHolder_returningKeyMatchingIdColumn_whenKeyHolderContainsMultipleKeysPerRecord() {

		Long idValue = 123L;
		when(jdbcOperations.batchUpdate(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put(idColumn.getReference(), idValue);
			keys.put("other", "someOtherValue");
			keyHolder.getKeyList().add(keys);
			return null;
		});
		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, false, true), jdbcOperations, idColumn);

		Object[] ids = batchInsertStrategy.execute(sql, sqlParameterSources);

		assertThat(ids).containsExactly(idValue);
	}

	@Test
	void insertsWithKeyHolder_returningNull__whenKeyHolderContainsMultipleKeysPerRecord_butIdColumnIsNull() {

		Long idValue = 123L;
		when(jdbcOperations.batchUpdate(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put(idColumn.getReference(), idValue);
			keys.put("other", "someOtherValue");
			keyHolder.getKeyList().add(keys);
			return null;
		});
		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, false, true), jdbcOperations, null);

		Object[] ids = batchInsertStrategy.execute(sql, sqlParameterSources);

		assertThat(ids).hasSize(sqlParameterSources.length);
		assertThat(ids).containsOnlyNulls();
	}

	@Test
	void insertsWithKeyHolder_returningNull_whenKeyHolderHasNoKeys() {

		BatchInsertStrategy batchInsertStrategy = new IdGeneratingBatchInsertStrategy(insertStrategy,
				createDialect(identifierProcessing, false, true), jdbcOperations, idColumn);

		Object[] ids = batchInsertStrategy.execute(sql, sqlParameterSources);

		assertThat(ids).hasSize(sqlParameterSources.length);
		assertThat(ids).containsOnlyNulls();
	}

	private static Dialect createDialect(final IdentifierProcessing identifierProcessing,
			final boolean requiresKeyColumnNames, final boolean supportsIdGenerationForBatchOperations) {

		return new AbstractDialect() {

			@Override
			public LimitClause limit() {
				return null;
			}

			@Override
			public LockClause lock() {
				return null;
			}

			@Override
			public IdentifierProcessing getIdentifierProcessing() {
				return identifierProcessing;
			}

			@Override
			public IdGeneration getIdGeneration() {

				return new IdGeneration() {

					@Override
					public boolean driverRequiresKeyColumnNames() {
						return requiresKeyColumnNames;
					}

					@Override
					public boolean supportedForBatchOperations() {
						return supportsIdGenerationForBatchOperations;
					}
				};
			}
		};
	}
}
