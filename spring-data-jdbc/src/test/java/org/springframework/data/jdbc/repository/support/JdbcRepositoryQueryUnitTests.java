/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.AfterLoadCallback;
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
 * @author Mark Paluch
 */
public class JdbcRepositoryQueryUnitTests {

	JdbcQueryMethod queryMethod;

	RowMapper<?> defaultRowMapper;
	ResultSetExtractor<?> defaultResultSetExtractor;
	NamedParameterJdbcOperations operations;
	ApplicationEventPublisher publisher;
	EntityCallbacks callbacks;
	RelationalMappingContext context;
	JdbcConverter converter;

	@Before
	public void setup() throws NoSuchMethodException {

		this.queryMethod = mock(JdbcQueryMethod.class);

		Parameters<?, ?> parameters = new DefaultParameters(
				JdbcRepositoryQueryUnitTests.class.getDeclaredMethod("dummyMethod"));
		doReturn(parameters).when(queryMethod).getParameters();

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.publisher = mock(ApplicationEventPublisher.class);
		this.callbacks = mock(EntityCallbacks.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new BasicJdbcConverter(context, mock(RelationResolver.class));
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		doReturn(null).when(queryMethod).getDeclaredQuery();

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(
						() -> new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
								.execute(new Object[] {}));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(RowMapper.class).when(queryMethod).getRowMapperClass();
		JdbcRepositoryQuery query = new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations,
				defaultRowMapper, converter);

		query.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedForNull() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		JdbcRepositoryQuery query = new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations,
				defaultRowMapper, converter);

		query.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), eq(defaultRowMapper));
	}

	@Test // DATAJDBC-165
	public void customRowMapperIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
				.execute(new Object[] {});

		verify(operations) //
				.queryForObject(anyString(), any(SqlParameterSource.class), isA(CustomRowMapper.class));
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorIsUsedWhenSpecified() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
				.execute(new Object[] {});

		ArgumentCaptor<CustomResultSetExtractor> captor = ArgumentCaptor.forClass(CustomResultSetExtractor.class);

		verify(operations).query(anyString(), any(SqlParameterSource.class), captor.capture());

		assertThat(captor.getValue()) //
				.isInstanceOf(CustomResultSetExtractor.class) // not verified by the captor
				.matches(crse -> crse.rowMapper == null, "RowMapper is expected to be null.");
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorAndRowMapperGetCombined() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(CustomResultSetExtractor.class).when(queryMethod).getResultSetExtractorClass();
		doReturn(CustomRowMapper.class).when(queryMethod).getRowMapperClass();

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
				.execute(new Object[] {});

		ArgumentCaptor<CustomResultSetExtractor> captor = ArgumentCaptor.forClass(CustomResultSetExtractor.class);

		verify(operations).query(anyString(), any(SqlParameterSource.class), captor.capture());

		assertThat(captor.getValue()) //
				.isInstanceOf(CustomResultSetExtractor.class) // not verified by the captor
				.matches(crse -> crse.rowMapper != null, "RowMapper is not expected to be null");
	}

	@Test // DATAJDBC-263, DATAJDBC-354
	public void publishesSingleEventWhenQueryReturnsSingleAggregate() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(false).when(queryMethod).isCollectionQuery();
		doReturn(new DummyEntity(1L)).when(operations).queryForObject(anyString(), any(SqlParameterSource.class),
				any(RowMapper.class));
		doReturn(true).when(context).hasPersistentEntityFor(DummyEntity.class);
		when(context.getRequiredPersistentEntity(DummyEntity.class).getIdentifierAccessor(any()).getIdentifier())
				.thenReturn("some identifier");

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
				.execute(new Object[] {});

		verify(publisher).publishEvent(any(AfterLoadEvent.class));
	}

	@Test // DATAJDBC-263, DATAJDBC-354
	public void publishesAsManyEventsAsReturnedAggregates() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(true).when(queryMethod).isCollectionQuery();
		doReturn(Arrays.asList(new DummyEntity(1L), new DummyEntity(1L))).when(operations).query(anyString(),
				any(SqlParameterSource.class), any(RowMapper.class));
		doReturn(true).when(context).hasPersistentEntityFor(DummyEntity.class);
		when(context.getRequiredPersistentEntity(DummyEntity.class).getIdentifierAccessor(any()).getIdentifier())
				.thenReturn("some identifier");

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter)
				.execute(new Object[] {});

		verify(publisher, times(2)).publishEvent(any(AfterLoadEvent.class));
	}

	@Test // DATAJDBC-400
	public void publishesCallbacks() {

		doReturn("some sql statement").when(queryMethod).getDeclaredQuery();
		doReturn(false).when(queryMethod).isCollectionQuery();
		DummyEntity dummyEntity = new DummyEntity(1L);
		doReturn(dummyEntity).when(operations).queryForObject(anyString(), any(SqlParameterSource.class),
				any(RowMapper.class));
		doReturn(true).when(context).hasPersistentEntityFor(DummyEntity.class);
		when(context.getRequiredPersistentEntity(DummyEntity.class).getIdentifierAccessor(any()).getIdentifier())
				.thenReturn("some identifier");

		new JdbcRepositoryQuery(publisher, callbacks, context, queryMethod, operations, defaultRowMapper, converter).execute(new Object[] {});

		verify(publisher).publishEvent(any(AfterLoadEvent.class));
		verify(callbacks).callback(AfterLoadCallback.class, dummyEntity);

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
