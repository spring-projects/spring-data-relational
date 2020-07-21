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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Point;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.Param;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link StringBasedR2dbcQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedR2dbcQueryUnitTests {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock private DatabaseClient databaseClient;
	@Mock private GenericExecuteSpec bindSpec;

	private RelationalMappingContext mappingContext;
	private MappingR2dbcConverter converter;
	private ProjectionFactory factory;
	private RepositoryMetadata metadata;

	@Before
	public void setUp() {

		this.mappingContext = new R2dbcMappingContext();
		this.converter = new MappingR2dbcConverter(this.mappingContext);
		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.factory = new SpelAwareProxyProjectionFactory();

		when(bindSpec.bind(anyInt(), any())).thenReturn(bindSpec);
		when(bindSpec.bind(anyString(), any())).thenReturn(bindSpec);
	}

	@Test
	public void bindsSimplePropertyCorrectly() {

		StringBasedR2dbcQuery query = getQueryMethod("findByLastname", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind(0, "White");
	}

	@Test // gh-164
	public void bindsPositionalPropertyCorrectly() {

		StringBasedR2dbcQuery query = getQueryMethod("findByLastnamePositional", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind(0, "White");
	}

	@Test
	public void bindsByNamedParameter() {

		StringBasedR2dbcQuery query = getQueryMethod("findByNamedParameter", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :lastname");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("lastname", "White");
	}

	@Test
	public void bindsByBindmarker() {

		StringBasedR2dbcQuery query = getQueryMethod("findByNamedBindMarker", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = @lastname");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("lastname", "White");
	}

	@Test
	public void bindsByIndexWithNamedParameter() {

		StringBasedR2dbcQuery query = getQueryMethod("findNotByNamedBindMarker", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :unknown");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind(0, "White");
	}

	@Test // gh-164
	public void bindsSimpleSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleSpel");
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod());

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("__synthetic_0__", "hello");
	}

	@Test // gh-164
	public void bindsIndexedSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleIndexedSpel", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("__synthetic_0__", "White");
		verifyNoMoreInteractions(bindSpec);
	}

	@Test // gh-164
	public void bindsPositionalSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simplePositionalSpel", String.class, String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White", "Walter");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get())
				.isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__ and firstname = :firstname");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("__synthetic_0__", "White");
		verify(bindSpec).bind("firstname", "Walter");
		verifyNoMoreInteractions(bindSpec);
	}

	@Test // gh-164
	public void bindsPositionalNamedSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("simpleNamedSpel", String.class, String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White", "Walter");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get())
				.isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__ and firstname = :firstname");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("__synthetic_0__", "White");
		verify(bindSpec).bind("firstname", "Walter");
		verifyNoMoreInteractions(bindSpec);
	}

	@Test // gh-164
	public void bindsComplexSpelQuery() {

		StringBasedR2dbcQuery query = getQueryMethod("queryWithSpelObject", Person.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), new Person("Walter"));

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :__synthetic_0__");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("__synthetic_0__", "Walter");
		verifyNoMoreInteractions(bindSpec);
	}

	@Test // gh-321
	public void skipsNonBindableParameters() {

		StringBasedR2dbcQuery query = getQueryMethod("queryWithUnusedParameter", String.class, Sort.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "Walter", null);

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = :name");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind(0, "Walter");
		verifyNoMoreInteractions(bindSpec);
	}

	private StringBasedR2dbcQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		R2dbcQueryMethod queryMethod = new R2dbcQueryMethod(method, metadata, factory, converter.getMappingContext());

		return new StringBasedR2dbcQuery(queryMethod, databaseClient, converter, PARSER,
				ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);
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
	}

	static class Person {

		String name;

		public Person(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
