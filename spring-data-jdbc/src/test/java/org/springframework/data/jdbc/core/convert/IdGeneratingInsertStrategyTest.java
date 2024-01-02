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
 * Unit tests for {@link IdGeneratingInsertStrategy}.
 *
 * @author Chirag Tailor
 */
class IdGeneratingInsertStrategyTest {

	SqlIdentifier idColumn = SqlIdentifier.quoted("id");
	IdentifierProcessing identifierProcessing = IdentifierProcessing.ANSI;
	NamedParameterJdbcOperations namedParameterJdbcOperations = mock(NamedParameterJdbcOperations.class);
	String sql = "some sql";
	SqlParameterSource sqlParameterSource = new SqlIdentifierParameterSource();

	@Test
	void insertsWithKeyHolderAndKeyColumnNames_whenDriverRequiresKeyColumnNames() {

		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, true),
				namedParameterJdbcOperations, idColumn);

		insertStrategy.execute(sql, sqlParameterSource);

		verify(namedParameterJdbcOperations).update(eq(sql), eq(sqlParameterSource), any(KeyHolder.class),
				eq(new String[] { idColumn.getReference() }));
	}

	@Test
	void insertsWithKeyHolder_whenDriverRequiresKeyColumnNames_butIdColumnIsNull() {

		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, true),
				namedParameterJdbcOperations, null);

		insertStrategy.execute(sql, sqlParameterSource);

		verify(namedParameterJdbcOperations).update(eq(sql), eq(sqlParameterSource), any(KeyHolder.class));
	}

	@Test
	void insertsWithKeyHolder_whenDriverDoesNotRequireKeyColumnNames() {

		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, false),
				namedParameterJdbcOperations, idColumn);

		insertStrategy.execute(sql, sqlParameterSource);

		verify(namedParameterJdbcOperations).update(eq(sql), eq(sqlParameterSource), any(KeyHolder.class));
	}

	@Test
	void insertsWithKeyHolder_returningKey_whenThereIsOnlyOne() {

		Long idValue = 123L;
		when(namedParameterJdbcOperations.update(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put("anything", idValue);
			keyHolder.getKeyList().add(keys);
			return null;
		});
		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, false),
				namedParameterJdbcOperations, idColumn);

		Object id = insertStrategy.execute(sql, sqlParameterSource);

		assertThat(id).isEqualTo(idValue);
	}

	@Test
	void insertsWithKeyHolder_returningKeyMatchingIdColumn_whenKeyHolderContainsMultipleKeysPerRecord() {

		Long idValue = 123L;
		when(namedParameterJdbcOperations.update(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put(idColumn.getReference(), idValue);
			keys.put("other", "someOtherValue");
			keyHolder.getKeyList().add(keys);
			return null;
		});
		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, false),
				namedParameterJdbcOperations, idColumn);

		Object id = insertStrategy.execute(sql, sqlParameterSource);

		assertThat(id).isEqualTo(idValue);
	}

	@Test
	void insertsWithKeyHolder_returningNull__whenKeyHolderContainsMultipleKeysPerRecord_butIdColumnIsNull() {

		Long idValue = 123L;
		when(namedParameterJdbcOperations.update(any(), any(), any())).thenAnswer(invocationOnMock -> {

			KeyHolder keyHolder = invocationOnMock.getArgument(2);
			HashMap<String, Object> keys = new HashMap<>();
			keys.put(idColumn.getReference(), idValue);
			keys.put("other", "someOtherValue");
			keyHolder.getKeyList().add(keys);
			return null;
		});
		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, false),
				namedParameterJdbcOperations, null);

		Object id = insertStrategy.execute(sql, sqlParameterSource);

		assertThat(id).isNull();
	}

	@Test
	void insertsWithKeyHolder_returningNull_whenKeyHolderHasNoKeys() {

		InsertStrategy insertStrategy = new IdGeneratingInsertStrategy(createDialect(identifierProcessing, false),
				namedParameterJdbcOperations, idColumn);

		Object id = insertStrategy.execute(sql, sqlParameterSource);

		assertThat(id).isNull();
	}

	private static Dialect createDialect(final IdentifierProcessing identifierProcessing,
			final boolean requiresKeyColumnNames) {

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
				};
			}
		};
	}
}
