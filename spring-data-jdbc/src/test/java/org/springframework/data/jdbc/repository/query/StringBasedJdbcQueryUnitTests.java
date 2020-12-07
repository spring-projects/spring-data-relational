/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.Parameters;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link StringBasedJdbcQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 * @author Mark Paluch
 */
public class StringBasedJdbcQueryUnitTests {

	JdbcQueryMethod queryMethod;

	RowMapper<Object> defaultRowMapper;
	NamedParameterJdbcOperations operations;
	RelationalMappingContext context;
	JdbcConverter converter;

	@BeforeEach
	public void setup() throws NoSuchMethodException {

		this.queryMethod = mock(JdbcQueryMethod.class);

		Parameters<?, ?> parameters = new RelationalParameters(
				StringBasedJdbcQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		doReturn(parameters).when(queryMethod).getParameters();

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new BasicJdbcConverter(context, mock(RelationResolver.class));
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		doReturn(null).when(queryMethod).getDeclaredQuery();

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> createQuery()
						.execute(new Object[] {}));
	}

	private StringBasedJdbcQuery createQuery() {

		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter);
		return query;
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(RowMapper.class).when(queryMethod).getRowMapperClass();
		StringBasedJdbcQuery query = createQuery();

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	public void defaultRowMapperIsUsedForNull() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		StringBasedJdbcQuery query = createQuery();

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	public void customRowMapperIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		StringBasedJdbcQuery query = createQuery();

		assertThat(query.determineRowMapper(defaultRowMapper)).isInstanceOf(CustomRowMapper.class);
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();

		createQuery().execute(new Object[] {});

		StringBasedJdbcQuery query = createQuery();

		ResultSetExtractor<Object> resultSetExtractor = query.determineResultSetExtractor(defaultRowMapper);

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper == defaultRowMapper,
						"RowMapper is expected to be default.");
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorAndRowMapperGetCombined() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		StringBasedJdbcQuery query = createQuery();

		ResultSetExtractor<Object> resultSetExtractor = query
				.determineResultSetExtractor(query.determineRowMapper(defaultRowMapper));

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper instanceof CustomRowMapper,
						"RowMapper is not expected to be custom");
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

	private static class CustomResultSetExtractor implements ResultSetExtractor<Object> {

		private final RowMapper rowMapper;

		CustomResultSetExtractor() {
			rowMapper = null;
		}

		public CustomResultSetExtractor(RowMapper rowMapper) {

			this.rowMapper = rowMapper;
		}

		@Override
		public Object extractData(ResultSet rs) throws DataAccessException {
			return null;
		}
	}

	private static class DummyEntity {
		private Long id;

		public DummyEntity(Long id) {
			this.id = id;
		}

		Long getId() {
			return id;
		}
	}
}
