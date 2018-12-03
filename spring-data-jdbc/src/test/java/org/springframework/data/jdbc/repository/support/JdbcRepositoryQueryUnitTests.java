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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.jdbc.support.RowMapperOrResultsetExtractor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.Parameters;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link JdbcRepositoryQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 */
public class JdbcRepositoryQueryUnitTests {

	JdbcQueryMethod queryMethod;

	RowMapper<?> defaultRowMapper;
	ResultSetExtractor<?> defaultResultSetExtractor;
	JdbcRepositoryQuery query;
	NamedParameterJdbcOperations operations;
	ApplicationEventPublisher publisher;
	RelationalMappingContext context;

	@Before
	public void setup() throws NoSuchMethodException {

		this.queryMethod = mock(JdbcQueryMethod.class);

		Parameters<?, ?> parameters = new DefaultParameters(
				JdbcRepositoryQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		doReturn(parameters).when(queryMethod).getParameters();

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.publisher = mock(ApplicationEventPublisher.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);

		this.query = new JdbcRepositoryQuery(publisher, context, queryMethod, operations, RowMapperOrResultsetExtractor.of(defaultRowMapper));
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

		new JdbcRepositoryQuery(publisher, context, queryMethod, operations, RowMapperOrResultsetExtractor.of(defaultRowMapper)).execute(new Object[] {});

		verify(operations) //
				.queryForObject(anyString(), any(SqlParameterSource.class), isA(CustomRowMapper.class));
	}
	
	@Test // DATAJDBC-290
	public void customResultSetExtractorIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();

		new JdbcRepositoryQuery(publisher, context, queryMethod, operations, RowMapperOrResultsetExtractor.of(defaultRowMapper)).execute(new Object[] {});

		verify(operations) //
				.query(anyString(), any(SqlParameterSource.class), isA(CustomResultSetExtractor.class));
	}


	@Test // DATAJDBC-263
	public void publishesSingleEventWhenQueryReturnsSingleAggregate() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();
		doReturn(false).when(queryMethod).isCollectionQuery();
		doReturn(new DummyEntity(1L)).when(operations).queryForObject(anyString(), any(SqlParameterSource.class), any(RowMapper.class));
		doReturn(true).when(context).hasPersistentEntityFor(DummyEntity.class);
		when(context.getRequiredPersistentEntity(DummyEntity.class).getIdentifierAccessor(any()).getRequiredIdentifier()).thenReturn("some identifier");

		new JdbcRepositoryQuery(publisher, context, queryMethod, operations, RowMapperOrResultsetExtractor.of(defaultRowMapper)).execute(new Object[] {});

		verify(publisher).publishEvent(any(AfterLoadEvent.class));
	}

	@Test // DATAJDBC-263
	public void publishesAsManyEventsAsReturnedAggregates() {

		doReturn("some sql statement").when(queryMethod).getAnnotatedQuery();
		doReturn(true).when(queryMethod).isCollectionQuery();
		doReturn(Arrays.asList(new DummyEntity(1L), new DummyEntity(1L))).when(operations).query(anyString(), any(SqlParameterSource.class), any(RowMapper.class));
		doReturn(true).when(context).hasPersistentEntityFor(DummyEntity.class);
		when(context.getRequiredPersistentEntity(DummyEntity.class).getIdentifierAccessor(any()).getRequiredIdentifier()).thenReturn("some identifier");

		new JdbcRepositoryQuery(publisher, context, queryMethod, operations, RowMapperOrResultsetExtractor.of(defaultRowMapper)).execute(new Object[] {});

		verify(publisher, times(2)).publishEvent(any(AfterLoadEvent.class));
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

		@Override
		public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
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
