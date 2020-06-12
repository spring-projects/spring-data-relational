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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessException;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link StringBasedJdbcQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 * @author Mark Paluch
 * @author Christopher Klein
 */
public class StringBasedJdbcQueryUnitTests {

	JdbcQueryMethod queryMethod;

	RowMapper<Object> defaultRowMapper;
	NamedParameterJdbcOperations operations;
	RelationalMappingContext context;
	JdbcConverter converter;
	QueryMethodEvaluationContextProvider evaluationContextProvider;

	@Before
	public void setup() throws NoSuchMethodException {

		this.queryMethod = mock(JdbcQueryMethod.class);

		Parameters<?, ?> parameters = new RelationalParameters(
				StringBasedJdbcQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		doReturn(parameters).when(queryMethod).getParameters();

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new BasicJdbcConverter(context, mock(RelationResolver.class));
		this.evaluationContextProvider = mock(QueryMethodEvaluationContextProvider.class);
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		doReturn(null).when(queryMethod).getDeclaredQuery();

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider)
						.execute(new Object[] {}));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(RowMapper.class).when(queryMethod).getRowMapperClass();
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider);

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	public void defaultRowMapperIsUsedForNull() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider);

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	public void customRowMapperIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider);

		assertThat(query.determineRowMapper(defaultRowMapper)).isInstanceOf(CustomRowMapper.class);
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();

		new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider).execute(new Object[] {});

		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider);

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

		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter, evaluationContextProvider);

		ResultSetExtractor<Object> resultSetExtractor = query
				.determineResultSetExtractor(query.determineRowMapper(defaultRowMapper));

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper instanceof CustomRowMapper,
						"RowMapper is not expected to be custom");
	}


	@Test // DATAJDBC-397
	public void spelCanBeUsedInsideQueries() {

		List<EvaluationContextExtension> list = new ArrayList<>();
		list.add(new MyEvaluationContextProvider());
		QueryMethodEvaluationContextProvider evaluationContextProviderImpl = new ExtensionAwareQueryMethodEvaluationContextProvider(list);

		doReturn("SELECT * FROM table WHERE c = :#{myext.testValue} AND c2 = :#{myext.doSomething()}").when(queryMethod).getDeclaredQuery();
		StringBasedJdbcQuery sut = new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter,
				evaluationContextProviderImpl);

		ArgumentCaptor<SqlParameterSource> paramSource = ArgumentCaptor.forClass(SqlParameterSource.class);
		ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<RowMapper> rse = ArgumentCaptor.forClass(RowMapper.class);

		sut.execute(new Object[] { "myValue"});

		verify(this.operations).queryForObject(query.capture(), paramSource.capture(), rse.capture());

		assertThat(query.getValue(), is("SELECT * FROM table WHERE c = :__$synthetic$__1 AND c2 = :__$synthetic$__2"));
		assertThat(paramSource.getValue().getValue("__$synthetic$__1"), is("test-value1"));
		assertThat(paramSource.getValue().getValue("__$synthetic$__2"), is("test-value2"));
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
	
	// DATAJDBC-397
	static class MyEvaluationContextProvider implements EvaluationContextExtension {
		@Override
		public String getExtensionId() {
			return "myext";
		}

		public static class ExtensionRoot {
			public String getTestValue() {
				return "test-value1";
			}

			public String doSomething() {
				return "test-value2";
			}
		}

		public Object getRootObject() {
			return new ExtensionRoot();
		}
	}

}
