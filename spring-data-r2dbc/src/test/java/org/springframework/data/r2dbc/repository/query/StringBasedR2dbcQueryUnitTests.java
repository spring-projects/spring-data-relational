/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link StringBasedR2dbcQuery}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class StringBasedR2dbcQueryUnitTests {

	private static final ValueExpressionParser PARSER = ValueExpressionParser.create(SpelExpressionParser::new);

	@Mock private R2dbcEntityOperations entityOperations;
	@Mock private BindTarget bindTarget;

	private RelationalMappingContext mappingContext;
	private MappingR2dbcConverter converter;
	private ReactiveDataAccessStrategy accessStrategy;
	private ProjectionFactory factory;
	private RepositoryMetadata metadata;
	private MockEnvironment environment;

	@BeforeEach
	void setUp() {

		this.mappingContext = new R2dbcMappingContext();
		this.converter = new MappingR2dbcConverter(this.mappingContext);
		this.accessStrategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE, converter);
		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.factory = new SpelAwareProxyProjectionFactory();
		this.environment = new MockEnvironment();
	}

	@Test
	void bindsSimplePropertyCorrectly() {

		StringBasedR2dbcQuery query = getQueryMethod("findByLastname", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
	}

	@Test // gh-164
	void bindsPositionalPropertyCorrectly() {

		StringBasedR2dbcQuery query = getQueryMethod("findByLastnamePositional", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
	}

	@Test
	void bindsByNamedParameter() {

		StringBasedR2dbcQuery query = getQueryMethod("findByNamedParameter", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
	}

	@Test
	void bindsByBindmarker() {

		StringBasedR2dbcQuery query = getQueryMethod("findByNamedBindMarker", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = @lastname");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind("lastname", "White");
	}

	@Test
	void bindsByIndexWithNamedParameter() {

		StringBasedR2dbcQuery query = getQueryMethod("findNotByNamedBindMarker", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.getSource()).isEqualTo("SELECT * FROM person WHERE lastname = :unknown");
		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
	}

	@Test // gh-164
	void bindsSimpleSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleSpel");
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod());

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.getSource()).isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__");
		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "hello");
	}

	@Test // gh-164
	void bindsIndexedSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleIndexedSpel", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.getSource()).isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__");
		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test // gh-164
	void bindsPositionalSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simplePositionalSpel", String.class, String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White", "Walter");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.getSource())
				.isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__ and firstname = :firstname");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
		verify(bindTarget).bind(1, "Walter");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test // gh-164
	void bindsPositionalNamedSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleNamedSpel", String.class, String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White", "Walter");

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1 and firstname = $2");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "White");
		verify(bindTarget).bind(1, "Walter");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test // gh-164
	void bindsComplexSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("queryWithSpelObject", Person.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), new Person("Walter"));

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "Walter");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test // gh-321
	void skipsNonBindableParameters() {

		StringBasedR2dbcQuery query = getQueryMethod("queryWithUnusedParameter", String.class, Sort.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "Walter", null);

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "Walter");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test // gh-465
	void translatesEnumToDatabaseValue() {

		StringBasedR2dbcQuery query = getQueryMethod("queryWithEnum", MyEnum.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), MyEnum.INSTANCE);

		PreparedOperation<?> stringQuery = query.createQuery(accessor).block();
		stringQuery.bindTo(bindTarget);

		verify(bindTarget).bind(0, "INSTANCE");
		verifyNoMoreInteractions(bindTarget);
	}

	@Test
		// gh-475, GH-1687
	void usesProjectionTypeForInterfaceProjectionResultMapping() {

		StringBasedR2dbcQuery query = getQueryMethod("findAsInterfaceProjection");

		assertThat(query.getQueryMethod().getResultProcessor().getReturnedType()
				.getReturnedType()).isEqualTo(PersonProjection.class);
		assertThat(query.getQueryMethod().getResultProcessor().getReturnedType()
				.getDomainType()).isEqualTo(Person.class);
		assertThat(query.resolveResultType(query.getQueryMethod()
				.getResultProcessor())).isEqualTo(PersonProjection.class);
	}

	@Test // gh-475
	void usesDtoTypeForDtoResultMapping() {

		StringBasedR2dbcQuery query = getQueryMethod("findAsDtoProjection");

		assertThat(query.resolveResultType(query.getQueryMethod().getResultProcessor())).isEqualTo(PersonDto.class);
	}

	@Test // gh-612
	void selectsSimpleType() {

		LocalDate value = LocalDate.now();
		MockResult result = MockResult.builder()
				.row(MockRow.builder().identified(0, LocalDate.class, value).build()).build();

		StatementRecorder recorder = StatementRecorder.newInstance();
		recorder.addStubbing(s -> s.equals("SELECT MAX(DATE)"), result);

		DatabaseClient databaseClient = DatabaseClient.builder() //
				.connectionFactory(recorder) //
				.bindMarkers(PostgresDialect.INSTANCE.getBindMarkersFactory()).build();
		entityOperations = new R2dbcEntityTemplate(databaseClient, PostgresDialect.INSTANCE, converter);

		StringBasedR2dbcQuery query = getQueryMethod("findAllLocalDates");

		Flux<Object> flux = (Flux) query.execute(new Object[0]);

		flux.as(StepVerifier::create).expectNext(value).verifyComplete();
	}

	@Test // GH-1654
	void rejectsStringBasedLimitQuery() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> getQueryMethod("unsupportedLimitQuery", String.class, Limit.class));
	}

	private StringBasedR2dbcQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		R2dbcQueryMethod queryMethod = new R2dbcQueryMethod(method, metadata, factory, converter.getMappingContext());

		QueryMethodValueEvaluationContextAccessor accessor = new QueryMethodValueEvaluationContextAccessor(
				environment, Collections.emptySet());

		return new StringBasedR2dbcQuery(queryMethod, entityOperations, converter, accessStrategy, new ValueExpressionDelegate(accessor, PARSER));
	}

	@SuppressWarnings("unused")
	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname = $1")
		Person findByLastname(@Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname = $1")
		Person findByLastnamePositional(String lastname);

		@Query("SELECT * FROM person WHERE lastname = :lastname")
		Person findByNamedParameter(@Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname = :unknown")
		Person findNotByNamedBindMarker(String lastname);

		@Query("SELECT * FROM person WHERE lastname = @lastname")
		Person findByNamedBindMarker(@Param("lastname") String lastname);

		@Query("SELECT * FROM person WHERE lastname = :#{'hello'}")
		Person simpleSpel();

		@Query("SELECT * FROM person WHERE lastname = :#{[0]}")
		Person simpleIndexedSpel(String value);

		@Query("SELECT * FROM person WHERE lastname = :#{[0]} and firstname = :firstname")
		Person simpleNamedSpel(@Param("value") String value, @Param("firstname") String firstname);

		@Query("SELECT * FROM person WHERE lastname = :#{#value} and firstname = :firstname")
		Person simplePositionalSpel(@Param("value") String value, @Param("firstname") String firstname);

		@Query("SELECT * FROM person WHERE lastname = :#{#person.name}")
		Person queryWithSpelObject(@Param("person") Person person);

		@Query("SELECT * FROM person WHERE lastname = :name")
		Person queryWithUnusedParameter(String name, Sort unused);

		@Query("SELECT * FROM person WHERE lastname = :name")
		Person queryWithEnum(MyEnum myEnum);

		@Query("SELECT * FROM person")
		PersonDto findAsDtoProjection();

		@Query("SELECT * FROM person")
		PersonProjection findAsInterfaceProjection();

		@Query("SELECT MAX(DATE)")
		Flux<LocalDate> findAllLocalDates();

		@Query("SELECT * FROM person WHERE lastname = $1")
		Person unsupportedLimitQuery(@Param("lastname") String lastname, Limit limit);
	}

	static class PersonDto {

	}

	interface PersonProjection {}

	static class Person {

		String name;

		Person(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	enum MyEnum {
		INSTANCE
	}
}
