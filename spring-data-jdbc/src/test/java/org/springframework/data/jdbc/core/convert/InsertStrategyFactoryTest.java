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
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link InsertStrategyFactory}.
 *
 * @author Chirag Tailor
 */
class InsertStrategyFactoryTest {

	NamedParameterJdbcOperations namedParameterJdbcOperations = mock(NamedParameterJdbcOperations.class);
	InsertStrategyFactory insertStrategyFactory = new InsertStrategyFactory(namedParameterJdbcOperations,
			AnsiDialect.INSTANCE);

	String sql = "some sql";
	SqlParameterSource sqlParameterSource = new SqlIdentifierParameterSource();
	SqlParameterSource[] sqlParameterSources = new SqlParameterSource[] { sqlParameterSource };

	@Test
	void insertWithoutGeneratedIds() {

		Object id = insertStrategyFactory.insertStrategy(IdValueSource.GENERATED, null).execute(sql, sqlParameterSource);

		verify(namedParameterJdbcOperations).update(sql, sqlParameterSource);
		assertThat(id).isNull();
	}

	@Test
	void batchInsertWithoutGeneratedIds() {

		Object[] ids = insertStrategyFactory.batchInsertStrategy(IdValueSource.GENERATED, null).execute(sql,
				sqlParameterSources);

		verify(namedParameterJdbcOperations).batchUpdate(sql, sqlParameterSources);
		assertThat(ids).hasSize(sqlParameterSources.length);
		assertThat(ids).containsOnlyNulls();
	}

}
