/*
 * Copyright 2020-2021 the original author or authors.
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

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link StringBasedJdbcQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 * @author Mark Paluch
 * @author Dennis Effing
 */
public class StringBasedJdbcQueryUnitTests {


	RowMapper<Object> defaultRowMapper;
	NamedParameterJdbcOperations operations;
	RelationalMappingContext context;
	JdbcConverter converter;

	@BeforeEach
	public void setup() throws NoSuchMethodException {

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new BasicJdbcConverter(context, mock(RelationResolver.class));
	}

	@Test // DATAJDBC-165
	public void emptyQueryThrowsException() {

		JdbcQueryMethod queryMethod = createMethod("noAnnotation");

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> createQuery(queryMethod)
						.execute(new Object[] {}));
	}

	@Test // DATAJDBC-165
	public void defaultRowMapperIsUsedByDefault() {

		JdbcQueryMethod queryMethod = createMethod("findAll");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	public void customRowMapperIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapper");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		assertThat(query.determineRowMapper(defaultRowMapper)).isInstanceOf(CustomRowMapper.class);
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor = query.determineResultSetExtractor(defaultRowMapper);

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper == defaultRowMapper,
						"RowMapper is expected to be default.");
	}

	@Test // DATAJDBC-290
	public void customResultSetExtractorAndRowMapperGetCombined() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapperAndResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor = query
				.determineResultSetExtractor(query.determineRowMapper(defaultRowMapper));

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper instanceof CustomRowMapper,
						"RowMapper is not expected to be custom");
	}

	@Test // DATAJDBC-356
	public void streamQueryCallsQueryForStreamOnOperations() {
		JdbcQueryMethod queryMethod = createMethod("findAllWithStreamReturnType");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		query.execute(new Object[] {});

		verify(operations).queryForStream(eq("some sql statement"), any(SqlParameterSource.class), any(RowMapper.class));
	}

	@Test // GH-774
	public void sliceQueryNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("sliceAll", Pageable.class);

		assertThatThrownBy(() -> new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Slice queries are not supported using string-based queries");
	}

	@Test // GH-774
	public void pageQueryNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("pageAll", Pageable.class);

		assertThatThrownBy(() -> new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Page queries are not supported using string-based queries");
	}

	private JdbcQueryMethod createMethod(String methodName, Class<?>... paramTypes) {

		Method method = ReflectionUtils.findMethod(MyRepository.class, methodName, paramTypes);
		return new JdbcQueryMethod(method, new DefaultRepositoryMetadata(MyRepository.class),
				new SpelAwareProxyProjectionFactory(), new PropertiesBasedNamedQueries(new Properties()), this.context);
	}

	private StringBasedJdbcQuery createQuery(JdbcQueryMethod queryMethod) {
		return new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter);
	}

	interface MyRepository extends Repository<Object, Long> {

		@Query(value = "some sql statement")
		List<Object> findAll();

		@Query(value = "some sql statement", rowMapperClass = CustomRowMapper.class)
		List<Object> findAllWithCustomRowMapper();

		@Query(value = "some sql statement", resultSetExtractorClass = CustomResultSetExtractor.class)
		List<Object> findAllWithCustomResultSetExtractor();

		@Query(value = "some sql statement", rowMapperClass = CustomRowMapper.class,
				resultSetExtractorClass = CustomResultSetExtractor.class)
		List<Object> findAllWithCustomRowMapperAndResultSetExtractor();

		@Query(value = "some sql statement")
		Stream<Object> findAllWithStreamReturnType();

		List<Object> noAnnotation();

		@Query(value = "some sql statement")
		Page<Object> pageAll(Pageable pageable);

		@Query(value = "some sql statement")
		Slice<Object> sliceAll(Pageable pageable);

	}

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
