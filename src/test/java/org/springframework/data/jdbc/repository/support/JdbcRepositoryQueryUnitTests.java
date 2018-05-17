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

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.jdbc.core.mapping.model.JdbcMappingContext;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.Parameters;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link JdbcRepositoryQuery}.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryQueryUnitTests {

	JdbcQueryMethod queryMethod;
	JdbcMappingContext context;
	RowMapper defaultRowMapper;
	JdbcRepositoryQuery query;

	@Before
	public void setup() throws NoSuchMethodException {

		Parameters parameters = new DefaultParameters(JdbcRepositoryQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		queryMethod = mock(JdbcQueryMethod.class);
		when(queryMethod.getParameters()).thenReturn(parameters);

		context = mock(JdbcMappingContext.class, RETURNS_DEEP_STUBS);
		defaultRowMapper = mock(RowMapper.class);
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		when(queryMethod.getAnnotatedQuery()).thenReturn(null);
		query = new JdbcRepositoryQuery(queryMethod, context, defaultRowMapper);

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> query.execute(new Object[]{}));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		when(queryMethod.getAnnotatedQuery()).thenReturn("some sql statement");
		when(queryMethod.getRowMapperClass()).thenReturn((Class) RowMapper.class);
		query = new JdbcRepositoryQuery(queryMethod, context, defaultRowMapper);

		query.execute(new Object[]{});

		verify(context.getTemplate()).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedForNull() {

		when(queryMethod.getAnnotatedQuery()).thenReturn("some sql statement");
		query = new JdbcRepositoryQuery(queryMethod, context, defaultRowMapper);

		query.execute(new Object[]{});

		verify(context.getTemplate()).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void customRowMapperIsUsedWhenSpecified() {

		when(queryMethod.getAnnotatedQuery()).thenReturn("some sql statement");
		when(queryMethod.getRowMapperClass()).thenReturn((Class) CustomRowMapper.class);
		query = new JdbcRepositoryQuery(queryMethod, context, defaultRowMapper);

		query.execute(new Object[]{});

		verify(context.getTemplate()).queryForObject(anyString(), any(SqlParameterSource.class), isA(CustomRowMapper.class));
	}

	/**
	 * The whole purpose of this method is to easily generate a {@link DefaultParameters} instance during test setup.
	 */
	private void dummyMethod() {
	}

	private static class CustomRowMapper implements RowMapper {
		@Override
		public Object mapRow(ResultSet rs, int rowNum) {
			return null;
		}
	}
}
