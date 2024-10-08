/*
 * Copyright 2020-2024 the original author or authors.
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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.dao.DataAccessException;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.test.util.ReflectionTestUtils;
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
 * @author Chirag Tailor
 * @author Christopher Klein
 * @author Marcin Grzejszczak
 */
class StringBasedJdbcQueryUnitTests {

	RowMapper<Object> defaultRowMapper;
	NamedParameterJdbcOperations operations;
	RelationalMappingContext context;
	JdbcConverter converter;
	ValueExpressionDelegate delegate;

	@BeforeEach
	void setup() {

		this.defaultRowMapper = mock(RowMapper.class);
		this.operations = mock(NamedParameterJdbcOperations.class);
		this.context = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
		this.converter = new MappingJdbcConverter(context, mock(RelationResolver.class));
		this.delegate = ValueExpressionDelegate.create();
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

		assertThat(query.determineRowMapper(queryMethod.getResultProcessor(), false)).isEqualTo(defaultRowMapper);
	}

	@Test // DATAJDBC-165, DATAJDBC-318
	void customRowMapperIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapper");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		assertThat(query.determineRowMapper(queryMethod.getResultProcessor(), false)).isInstanceOf(CustomRowMapper.class);
	}

	@Test // DATAJDBC-290
	void customResultSetExtractorIsUsedWhenSpecified() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor1 = query.determineResultSetExtractor(() -> defaultRowMapper);
		ResultSetExtractor<Object> resultSetExtractor2 = query.determineResultSetExtractor(() -> defaultRowMapper);

		assertThat(resultSetExtractor1) //
				.isInstanceOf(CustomResultSetExtractor.class) //
				.matches(crse -> ((CustomResultSetExtractor) crse).rowMapper == defaultRowMapper,
						"RowMapper is expected to be default.");

		assertThat(resultSetExtractor1).isNotSameAs(resultSetExtractor2);
	}

	@Test // GH-1721
	void cachesCustomMapperAndExtractorInstances() {

		JdbcQueryMethod queryMethod = createMethod("findAllCustomRowMapperResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor1 = query.determineResultSetExtractor(() -> {
			throw new UnsupportedOperationException();
		});

		ResultSetExtractor<Object> resultSetExtractor2 = query.determineResultSetExtractor(() -> {
			throw new UnsupportedOperationException();
		});

		assertThat(resultSetExtractor1).isSameAs(resultSetExtractor2);
		assertThat(resultSetExtractor1).extracting("rowMapper").isInstanceOf(CustomRowMapper.class);

		assertThat(ReflectionTestUtils.getField(resultSetExtractor1, "rowMapper"))
				.isSameAs(ReflectionTestUtils.getField(resultSetExtractor2, "rowMapper"));
	}

	@Test // GH-1721
	void obtainsCustomRowMapperRef() {

		CustomRowMapper customRowMapper = new CustomRowMapper();
		JdbcQueryMethod queryMethod = createMethod("findAllCustomRowMapperRef");
		StringBasedJdbcQuery query = createQuery(queryMethod, "CustomRowMapper", customRowMapper);

		RowMapper<?> rowMapper = query.determineRowMapper(queryMethod.getResultProcessor(), false);
		ResultSetExtractor<Object> resultSetExtractor = query.determineResultSetExtractor(() -> {
			throw new UnsupportedOperationException();
		});

		assertThat(rowMapper).isSameAs(customRowMapper);
		assertThat(resultSetExtractor).isNull();
	}

	@Test // GH-1721
	void obtainsCustomResultSetExtractorRef() {

		CustomResultSetExtractor cre = new CustomResultSetExtractor();
		JdbcQueryMethod queryMethod = createMethod("findAllCustomResultSetExtractorRef");
		StringBasedJdbcQuery query = createQuery(queryMethod, "CustomResultSetExtractor", cre);

		RowMapper<?> rowMapper = query.determineRowMapper(queryMethod.getResultProcessor(), false);
		ResultSetExtractor<Object> resultSetExtractor = query.determineResultSetExtractor(() -> {
			throw new UnsupportedOperationException();
		});

		assertThat(rowMapper).isSameAs(defaultRowMapper);
		assertThat(resultSetExtractor).isSameAs(cre);
	}

	@Test // GH-1721
	void failsOnRowMapperRefAndClassDeclaration() {
		assertThatIllegalArgumentException().isThrownBy(() -> createQuery(createMethod("invalidMapperRefAndClass")))
				.withMessageContaining("Invalid RowMapper configuration");
	}

	@Test // GH-1721
	void failsOnResultSetExtractorRefAndClassDeclaration() {
		assertThatIllegalArgumentException().isThrownBy(() -> createQuery(createMethod("invalidExtractorRefAndClass")))
				.withMessageContaining("Invalid ResultSetExtractor configuration");
	}

	@Test // DATAJDBC-290
	void customResultSetExtractorAndRowMapperGetCombined() {

		JdbcQueryMethod queryMethod = createMethod("findAllWithCustomRowMapperAndResultSetExtractor");
		StringBasedJdbcQuery query = createQuery(queryMethod);

		ResultSetExtractor<Object> resultSetExtractor = query
				.determineResultSetExtractor(() -> query.determineRowMapper(queryMethod.getResultProcessor(), false));

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

		assertThatThrownBy(
				() -> new StringBasedJdbcQuery(queryMethod, operations,  result -> defaultRowMapper, converter, delegate))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Slice queries are not supported using string-based queries");
	}

	@Test // GH-774
	void pageQueryNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("pageAll", Pageable.class);

		assertThatThrownBy(
				() -> new StringBasedJdbcQuery(queryMethod, operations, result -> defaultRowMapper, converter, delegate))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("Page queries are not supported using string-based queries");
	}

	@Test // GH-1654
	void limitNotSupported() {

		JdbcQueryMethod queryMethod = createMethod("unsupportedLimitQuery", String.class, Limit.class);

		assertThatThrownBy(
				() -> new StringBasedJdbcQuery(queryMethod, operations, result -> defaultRowMapper, converter, delegate))
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test // GH-1212
	void convertsEnumCollectionParameterIntoStringCollectionParameter() {

		SqlParameterSource sqlParameterSource = forMethod("findByEnumTypeIn", Set.class)
				.withArguments(Set.of(Direction.LEFT, Direction.RIGHT)).extractParameterSource();

		assertThat(sqlParameterSource.getValue("directions")).asList().containsExactlyInAnyOrder("LEFT", "RIGHT");
	}

	@Test // GH-1212
	void convertsEnumCollectionParameterUsingCustomConverterWhenRegisteredForType() {

		SqlParameterSource sqlParameterSource = forMethod("findByEnumTypeIn", Set.class) //
				.withCustomConverters(DirectionToIntegerConverter.INSTANCE, IntegerToDirectionConverter.INSTANCE)
				.withArguments(Set.of(Direction.LEFT, Direction.RIGHT)) //
				.extractParameterSource();

		assertThat(sqlParameterSource.getValue("directions")).asList().containsExactlyInAnyOrder(-1, 1);
	}

	@Test // GH-1212
	void doesNotConvertNonCollectionParameter() {

		SqlParameterSource sqlParameterSource = forMethod("findBySimpleValue", Integer.class) //
				.withArguments(1) //
				.extractParameterSource();

		assertThat(sqlParameterSource.getValue("value")).isEqualTo(1);
	}

	@Test // GH-1343
	void appliesConverterToIterable() {

		SqlParameterSource sqlParameterSource = forMethod("findByListContainer", ListContainer.class) //
				.withCustomConverters(ListContainerToStringConverter.INSTANCE)
				.withArguments(new ListContainer("one", "two", "three")) //
				.extractParameterSource();

		assertThat(sqlParameterSource.getValue("value")).isEqualTo("one");
	}

	@Test // GH-1323
	void queryByListOfTuples() {

		String[][] tuples = { new String[] { "Albert", "Einstein" }, new String[] { "Richard", "Feynman" } };

		SqlParameterSource parameterSource = forMethod("findByListOfTuples", List.class) //
				.withArguments(Arrays.asList(tuples))//
				.extractParameterSource();

		assertThat(parameterSource.getValue("tuples")).asInstanceOf(LIST)//
				.containsExactly(tuples);

		assertThat(parameterSource.getSqlType("tuples")).isEqualTo(JdbcUtil.TYPE_UNKNOWN.getVendorTypeNumber());
	}

	@Test // GH-1323
	void queryByListOfConvertableTuples() {

		SqlParameterSource parameterSource = forMethod("findByListOfTuples", List.class) //
				.withCustomConverters(DirectionToIntegerConverter.INSTANCE) //
				.withArguments(
						Arrays.asList(new Object[] { Direction.LEFT, "Einstein" }, new Object[] { Direction.RIGHT, "Feynman" }))
				.extractParameterSource();

		assertThat(parameterSource.getValue("tuples")).asInstanceOf(LIST) //
				.containsExactly(new Object[][] { new Object[] { -1, "Einstein" }, new Object[] { 1, "Feynman" } });
	}

	@Test // GH-619
	void spelCanBeUsedInsideQueries() {

		JdbcQueryMethod queryMethod = createMethod("findBySpelExpression", Object.class);

		List<EvaluationContextExtension> list = new ArrayList<>();
		list.add(new MyEvaluationContextProvider());

		QueryMethodValueEvaluationContextAccessor accessor = new QueryMethodValueEvaluationContextAccessor(new StandardEnvironment(), list);
		this.delegate = new ValueExpressionDelegate(accessor, ValueExpressionParser.create());

		StringBasedJdbcQuery sut = new StringBasedJdbcQuery(queryMethod, operations, result -> defaultRowMapper, converter, delegate);

		ArgumentCaptor<SqlParameterSource> paramSource = ArgumentCaptor.forClass(SqlParameterSource.class);
		ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);

		sut.execute(new Object[] { "myValue" });

		verify(this.operations).queryForObject(query.capture(), paramSource.capture(), any(RowMapper.class));

		assertThat(query.getValue())
				.isEqualTo("SELECT * FROM table WHERE c = :__$synthetic$__1 AND c2 = :__$synthetic$__2");
		assertThat(paramSource.getValue().getValue("__$synthetic$__1")).isEqualTo("test-value1");
		assertThat(paramSource.getValue().getValue("__$synthetic$__2")).isEqualTo("test-value2");
	}

	QueryFixture forMethod(String name, Class... paramTypes) {
		return new QueryFixture(createMethod(name, paramTypes));
	}

	private class QueryFixture {

		private final JdbcQueryMethod method;
		private Object[] arguments;
		private MappingJdbcConverter converter;

		public QueryFixture(JdbcQueryMethod method) {
			this.method = method;
		}

		public QueryFixture withArguments(Object... arguments) {

			this.arguments = arguments;

			return this;
		}

		public SqlParameterSource extractParameterSource() {

			MappingJdbcConverter converter = this.converter == null //
					? new MappingJdbcConverter(mock(RelationalMappingContext.class), //
							mock(RelationResolver.class))
					: this.converter;

			StringBasedJdbcQuery query = new StringBasedJdbcQuery(method.getDeclaredQuery(), method, operations, result -> mock(RowMapper.class),
					converter, delegate);

			query.execute(arguments);

			ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);
			verify(operations).queryForObject(anyString(), captor.capture(), any(RowMapper.class));

			return captor.getValue();
		}

		public QueryFixture withConverter(MappingJdbcConverter converter) {

			this.converter = converter;

			return this;
		}

		public QueryFixture withCustomConverters(Object... converters) {

			return withConverter(new MappingJdbcConverter(mock(RelationalMappingContext.class), mock(RelationResolver.class),
					new JdbcCustomConversions(List.of(converters)), JdbcTypeFactory.unsupported()));
		}
	}

	private JdbcQueryMethod createMethod(String methodName, Class<?>... paramTypes) {

		Method method = ReflectionUtils.findMethod(MyRepository.class, methodName, paramTypes);
		return new JdbcQueryMethod(method, new DefaultRepositoryMetadata(MyRepository.class),
				new SpelAwareProxyProjectionFactory(), new PropertiesBasedNamedQueries(new Properties()), this.context);
	}

	private StringBasedJdbcQuery createQuery(JdbcQueryMethod queryMethod) {
		return createQuery(queryMethod, null, null);
	}

	private StringBasedJdbcQuery createQuery(JdbcQueryMethod queryMethod, String preparedReference, Object value) {
		return new StringBasedJdbcQuery(queryMethod, operations, new StubRowMapperFactory(preparedReference, value), converter, delegate);
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

		@Query(value = "some sql statement", rowMapperClass = CustomRowMapper.class,
				resultSetExtractorClass = CustomResultSetExtractor.class)
		Stream<Object> findAllCustomRowMapperResultSetExtractor();

		@Query(value = "some sql statement", rowMapperRef = "CustomRowMapper")
		Stream<Object> findAllCustomRowMapperRef();

		@Query(value = "some sql statement", resultSetExtractorRef = "CustomResultSetExtractor")
		Stream<Object> findAllCustomResultSetExtractorRef();

		@Query(value = "some sql statement", rowMapperRef = "CustomResultSetExtractor",
				rowMapperClass = CustomRowMapper.class)
		Stream<Object> invalidMapperRefAndClass();

		@Query(value = "some sql statement", resultSetExtractorRef = "CustomResultSetExtractor",
				resultSetExtractorClass = CustomResultSetExtractor.class)
		Stream<Object> invalidExtractorRefAndClass();

		List<Object> noAnnotation();

		@Query(value = "some sql statement")
		Page<Object> pageAll(Pageable pageable);

		@Query(value = "some sql statement")
		Slice<Object> sliceAll(Pageable pageable);

		@Query(value = "some sql statement")
		List<Object> findByEnumTypeIn(Set<Direction> directions);

		@Query(value = "some sql statement")
		List<Object> findBySimpleValue(Integer value);

		@Query(value = "some sql statement")
		List<Object> findByListContainer(ListContainer value);

		@Query("SELECT * FROM table WHERE c = :#{myext.testValue} AND c2 = :#{myext.doSomething()}")
		Object findBySpelExpression(Object object);

		@Query("SELECT * FROM person WHERE lastname = $1")
		Object unsupportedLimitQuery(@Param("lastname") String lastname, Limit limit);

		@Query("select count(1) from person where (firstname, lastname) in (:tuples)")
		Object findByListOfTuples(@Param("tuples") List<Object[]> tuples);
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

			int integer = switch (source) {
				case LEFT -> -1;
				case CENTER -> 0;
				case RIGHT -> 1;
			};
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

	static class ListContainer implements Iterable<String> {

		private final List<String> values;

		ListContainer(String... values) {
			this.values = List.of(values);
		}

		@Override
		public Iterator<String> iterator() {
			return values.iterator();
		}
	}

	@WritingConverter
	enum ListContainerToStringConverter implements Converter<ListContainer, String> {

		INSTANCE;

		@Override
		public String convert(ListContainer source) {
			return source.values.get(0);
		}
	}

	private static class DummyEntity {
		private final Long id;

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

		@Override
		public Object getRootObject() {
			return new ExtensionRoot();
		}
	}

	private class StubRowMapperFactory implements AbstractJdbcQuery.RowMapperFactory {

		private final String preparedReference;
		private final Object value;

		StubRowMapperFactory(String preparedReference, Object value) {
			this.preparedReference = preparedReference;
			this.value = value;
		}

		@Override
		public RowMapper<Object> create(Class<?> result) {
			return defaultRowMapper;
		}

		@Override
		public RowMapper<Object> getRowMapper(String reference) {

			if (preparedReference.equals(reference)) {
				return (RowMapper<Object>) value;
			}
			return AbstractJdbcQuery.RowMapperFactory.super.getRowMapper(reference);
		}

		@Override
		public ResultSetExtractor<Object> getResultSetExtractor(String reference) {

			if (preparedReference.equals(reference)) {
				return (ResultSetExtractor<Object>) value;
			}
			return AbstractJdbcQuery.RowMapperFactory.super.getResultSetExtractor(reference);
		}
	}
}
