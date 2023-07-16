/*
 * Copyright 2020-2023 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.Param;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.ReflectionUtils;

import lombok.Getter;

/**
 * Unit tests for {@link StringBasedJdbcQuery}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 * @author Mark Paluch
 * @author Dennis Effing
 * @author Chirag Tailor
 */
class StringBasedJdbcQueryUnitTests {

	RowMapper<Object> defaultRowMapper;
	NamedParameterJdbcOperations operations;
	RelationalMappingContext context;
	JdbcConverter converter;

	@BeforeEach
	void setup() {

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new BasicJdbcConverter(context, mock(RelationResolver.class));
	}

	@Test // DATAJDBC-165
	void emptyQueryThrowsException() {

		JdbcQueryMethod queryMethod = createMethod("noAnnotation");

		Assertions.assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> createQuery(queryMethod).execute(new Object[] {}));
	}

	@Test // DATAJDBC-165
	void defaultRowMapperIsUsedByDefault() {

		JdbcQueryMethod queryMethod = createMethod("findAll");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		assertThat(query.determineRowMapper(defaultRowMapper)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	void customRowMapperIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapper");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		assertThat(query.determineRowMapper(defaultRowMapper)).isInstanceOf(CustomRowMapper.class);
	}

	@Test // DATAJDBC-290
	void customResultSetExtractorIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor = query.determineResultSetExtractor(defaultRowMapper);

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper == defaultRowMapper,
						"RowMapper is expected to be default.");
	}

	@Test // DATAJDBC-290
	void customResultSetExtractorAndRowMapperGetCombined() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapperAndResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor = query
				.determineResultSetExtractor(query.determineRowMapper(defaultRowMapper));

		assertThat(resultSetExtractor) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper instanceof CustomRowMapper,
						"RowMapper is not expected to be custom");
	}

	@Test // GH-578
	void streamQueryCallsQueryForStreamOnOperations() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithStreamReturnType");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		query.execute(new Object[] {});

		verify(operations).queryForStream(eq("some sql statement"), any(SqlParameterSource.class), any(RowMapper.class));
	}

	@Test // GH-578
	void streamQueryFallsBackToCollectionQueryWhenCustomResultSetExtractorIsSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithStreamReturnTypeAndResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		query.execute(new Object[] {});

		ArgumentCaptor<ResultSetExtractor<?>> captor = ArgumentCaptor.forClass(ResultSetExtractor.class);
		verify(operations).query(eq("some sql statement"), any(SqlParameterSource.class), captor.capture());
		assertThat(captor.getValue()).isInstanceOf(CustomResultSetExtractor.class);
	}

	@Test // GH-774
	void sliceQueryNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("sliceAll", Pageable.class);

		assertThatThrownBy(() -> new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Slice queries are not supported using string-based queries");
	}

	@Test // GH-774
	void pageQueryNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("pageAll", Pageable.class);

		assertThatThrownBy(() -> new StringBasedJdbcQuery(queryMethod, operations, defaultRowMapper, converter))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Page queries are not supported using string-based queries");
	}

	@Test // GH-1212
	void convertsEnumCollectionParameterIntoStringCollectionParameter() {

		JdbcQueryMethod queryMethod = createMethod("findByEnumTypeIn", Set.class);
		BasicJdbcConverter converter = new BasicJdbcConverter(mock(RelationalMappingContext.class), mock(RelationResolver.class));
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, result -> mock(RowMapper.class), converter);

		query.execute(new Object[] { asList(Direction.LEFT, Direction.RIGHT) });

		ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
		verify(operations).queryForObject(anyString(), captor.capture(), any(RowMapper.class));

		SqlParameterSource sqlParameterSource = captor.getValue();
		assertThat(sqlParameterSource.getValue("directions")).asList().containsExactlyInAnyOrder("LEFT", "RIGHT");
	}

	@Test // GH-1212
	void convertsEnumCollectionParameterUsingCustomConverterWhenRegisteredForType() {

		JdbcQueryMethod queryMethod = createMethod("findByEnumTypeIn", Set.class);
		BasicJdbcConverter converter = new BasicJdbcConverter(mock(RelationalMappingContext.class), mock(RelationResolver.class), new JdbcCustomConversions(asList(DirectionToIntegerConverter.INSTANCE, IntegerToDirectionConverter.INSTANCE)), JdbcTypeFactory.unsupported(), IdentifierProcessing.ANSI);
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, result -> mock(RowMapper.class), converter);

		query.execute(new Object[] { asList(Direction.LEFT, Direction.RIGHT) });

		ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
		verify(operations).queryForObject(anyString(), captor.capture(), any(RowMapper.class));

		SqlParameterSource sqlParameterSource = captor.getValue();
		assertThat(sqlParameterSource.getValue("directions")).asList().containsExactlyInAnyOrder(-1, 1);
	}

	@Test // GH-1212
	void doesNotConvertNonCollectionParameter() {

		JdbcQueryMethod queryMethod = createMethod("findBySimpleValue", Integer.class);
		BasicJdbcConverter converter = new BasicJdbcConverter(mock(RelationalMappingContext.class), mock(RelationResolver.class));
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, result -> mock(RowMapper.class), converter);

		query.execute(new Object[] { 1 });

		ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
		verify(operations).queryForObject(anyString(), captor.capture(), any(RowMapper.class));

		SqlParameterSource sqlParameterSource = captor.getValue();
		assertThat(sqlParameterSource.getValue("value")).isEqualTo(1);
	}

	@Test
	void convertMapAndJavaBeanParameter() {
		JdbcQueryMethod queryMethod = createMethod("queryMethodWithQueryParameters", Map.class, PageInfo.class);
		BasicJdbcConverter converter = new BasicJdbcConverter(mock(RelationalMappingContext.class), mock(RelationResolver.class));
		StringBasedJdbcQuery query = new StringBasedJdbcQuery(queryMethod, operations, result -> mock(RowMapper.class), converter);

		Map<String, Object> queryParams = new HashMap<>(1);
		queryParams.put("status", "BLOCKED");

		PageInfo pageInfo = new PageInfo(5L, 15);

		query.execute(new Object[] { queryParams, pageInfo });

		ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
		verify(operations).queryForObject(anyString(), captor.capture(), any(RowMapper.class));

		SqlParameterSource sqlParameterSource = captor.getValue();
		assertTrue(sqlParameterSource.hasValue("queryParams.status"));
		assertEquals(queryParams.get("status"), sqlParameterSource.getValue("queryParams.status"));
		assertTrue(sqlParameterSource.hasValue("page.size"));
		assertEquals(pageInfo.getSize(), sqlParameterSource.getValue("page.size"));
		assertTrue(sqlParameterSource.hasValue("page.pageNumber"));
		assertEquals(pageInfo.getPageNumber(), sqlParameterSource.getValue("page.pageNumber"));
		assertTrue(sqlParameterSource.hasValue("page.offset"));
		assertEquals(pageInfo.getOffset(), sqlParameterSource.getValue("page.offset"));
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

		@Query(value = "some sql statement", resultSetExtractorClass = CustomResultSetExtractor.class)
		Stream<Object> findAllWithStreamReturnTypeAndResultSetExtractor();

		List<Object> noAnnotation();

		@Query(value = "some sql statement")
		Page<Object> pageAll(Pageable pageable);

		@Query(value = "some sql statement")
		Slice<Object> sliceAll(Pageable pageable);

		@Query(value = "some sql statement")
		List<Object> findByEnumTypeIn(Set<Direction> directions);

		@Query(value = "some sql statement")
		List<Object> findBySimpleValue(Integer value);

		@Query("SELECT something FROM table_name WHERE status = queryParams.status LIMIT page.size OFFSET page.offset")
		List<Object> queryMethodWithQueryParameters(
				@Multiparameter Map<String, ?> queryParams,
				@Param("page") @Multiparameter PageInfo pageInfo);
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

	private enum Direction {
		LEFT, CENTER, RIGHT
	}

	@WritingConverter
	enum DirectionToIntegerConverter implements Converter<Direction, JdbcValue> {

		INSTANCE;

		@Override
		public JdbcValue convert(Direction source) {

			int integer;
			switch (source) {
				case LEFT:
					integer = -1;
					break;
				case CENTER:
					integer = 0;
					break;
				case RIGHT:
					integer = 1;
					break;
				default:
					throw new IllegalArgumentException();
			}
			return JdbcValue.of(integer, JDBCType.INTEGER);
		}
	}

	@ReadingConverter
	enum IntegerToDirectionConverter implements Converter<Integer, Direction> {

		INSTANCE;

		@Override
		public Direction convert(Integer source) {

			if (source == 0) {
				return Direction.CENTER;
			} else if (source < 0) {
				return Direction.LEFT;
			} else {
				return Direction.RIGHT;
			}
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

	@Getter
	private static class PageInfo {
		private final long pageNumber;
		private final int size;
		private final long offset;
		public PageInfo(long pageNumber, int size) {
			this.pageNumber = pageNumber;
			this.size = size;
			this.offset = (pageNumber - 1) * size;
		}
	}
}
