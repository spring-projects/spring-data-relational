/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.support;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.Parameters;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link JdbcRepositoryQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
public class JdbcRepositoryQueryUnitTests {

	JdbcQueryMethod queryMethod;

	RowMapper<?> defaultRowMapper;
	JdbcRepositoryQuery query;
	NamedParameterJdbcOperations operations;

	@Before
	public void setup() throws NoSuchMethodException {

		this.queryMethod = mock(JdbcQueryMethod.class);

		Parameters<?, ?> parameters = new DefaultParameters(
				JdbcRepositoryQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		doReturn(parameters).when(queryMethod).getParameters();

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);

		this.query = new JdbcRepositoryQuery(queryMethod, operations, defaultRowMapper);
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		doReturn(null).when(queryMethod).getAnnotatedQuery();

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> query.execute(new Object[] {}));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();
		doReturn(RowMapper.class).when(queryMethod).getRowMapperClass();

		query.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedForNull() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();

		query.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void customRowMapperIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		new JdbcRepositoryQuery(queryMethod, operations, defaultRowMapper).execute(new Object[] {});

		verify(operations) //
				.queryForObject(anyString(), any(SqlParameterSource.class), isA(CustomRowMapper.class));
	}

	/**
	 * The whole purpose of this method is to easily generate a {@link DefaultParameters} instance during test setup.
	 */
	@SuppressWarnings("unused")
	private void dummyMethod() {}

	private static class CustomRowMapper implements RowMapper<Object> {

		@Override
		public Object mapRow(ResultSet rs, int rowNum) {
			return null;
		}
	}
}
