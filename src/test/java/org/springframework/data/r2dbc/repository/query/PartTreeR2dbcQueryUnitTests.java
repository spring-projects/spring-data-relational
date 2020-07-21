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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.annotation.Id;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit tests for {@link PartTreeR2dbcQuery}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @author Myeonghyeon Lee
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeR2dbcQueryUnitTests {

	private static final String TABLE = "users";
	private static final String ALL_FIELDS = TABLE + ".id, " + TABLE + ".first_name, " + TABLE + ".last_name, " + TABLE
			+ ".date_of_birth, " + TABLE + ".age, " + TABLE + ".active";
	private static final String DISTINCT = "DISTINCT";

	@Mock ConnectionFactory connectionFactory;
	@Mock R2dbcConverter r2dbcConverter;

	RelationalMappingContext mappingContext;
	ReactiveDataAccessStrategy dataAccessStrategy;
	DatabaseClient databaseClient;

	@Before
	public void setUp() {

		ConnectionFactoryMetadata metadataMock = mock(ConnectionFactoryMetadata.class);
		when(metadataMock.getName()).thenReturn("PostgreSQL");
		when(connectionFactory.getMetadata()).thenReturn(metadataMock);

		when(r2dbcConverter.writeValue(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

		mappingContext = new R2dbcMappingContext();
		doReturn(mappingContext).when(r2dbcConverter).getMappingContext();

		R2dbcDialect dialect = DialectResolver.getDialect(connectionFactory);
		dataAccessStrategy = new DefaultReactiveDataAccessStrategy(dialect, r2dbcConverter);

		databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory).build();
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "John" }));

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-282
	public void createsQueryWithIsNullCondition() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[] { null })));

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name IS NULL");
	}

	@Test // gh-282
	public void createsQueryWithLimitForExistsProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery query = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[] { "John" })));

		assertThat(query.get())
				.isEqualTo("SELECT " + TABLE + ".id FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameAndFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE
				+ ".last_name = $1 AND (" + TABLE + ".first_name = $2)");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByOneOfTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameOrFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE
				+ ".last_name = $1 OR (" + TABLE + ".first_name = $2)");
	}

	@Test // gh-282, gh-349
	public void createsQueryToFindAllEntitiesByDateAttributeBetween() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBetween", Date.class, Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		Date from = new Date();
		Date to = new Date();
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { from, to });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth BETWEEN $1 AND $2");

		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		when(bindSpecMock.bind(anyInt(), any())).thenReturn(bindSpecMock);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, from);
		verify(bindSpecMock, times(1)).bind(1, to);
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeLessThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age < $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeLessThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age <= $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age > $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age >= $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByDateAttributeAfter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthAfter", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth > $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByDateAttributeBefore() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBefore", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth < $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeIsNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NULL");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeIsNotNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNotNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NOT NULL");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeNotLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name NOT LIKE $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeStartingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	public void appendsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "Jo%");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeEndingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	public void prependsLikeOperatorParameterWithPercentSymbolForEndingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%hn");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	public void wrapsLikeOperatorParameterWithPercentSymbolsForContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%oh%");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeNotContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name NOT LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	public void wrapsLikeOperatorParameterWithPercentSymbolsForNotContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%oh%");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeWithDescendingOrderingByStringAttribute()
			throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameDesc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age = $1 ORDER BY last_name DESC");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeWithAscendingOrderingByStringAttribute() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameAsc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age = $1 ORDER BY last_name ASC");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeNot() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameNot", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Doe" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".last_name != $1");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeIn() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IN ($1)");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByIntegerAttributeNotIn() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeNotIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age NOT IN ($1)");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByBooleanAttributeTrue() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveTrue");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = TRUE");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByBooleanAttributeFalse() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveFalse");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = FALSE");
	}

	@Test // gh-282
	public void createsQueryToFindAllEntitiesByStringAttributeIgnoringCase() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameIgnoreCase", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE UPPER(" + TABLE + ".first_name) = UPPER($1)");
	}

	@Test // gh-282
	public void throwsExceptionWhenIgnoringCaseIsImpossible() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findByIdIgnoringCase", Long.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalStateException()
				.isThrownBy(() -> r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[] { 1L })));
	}

	@Test // gh-282
	public void throwsExceptionWhenInPredicateHasNonIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIn", Long.class);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter, dataAccessStrategy));
	}

	@Test // gh-282
	public void throwsExceptionWhenSimplePropertyPredicateHasIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllById", Collection.class);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter, dataAccessStrategy));
	}

	@Test // gh-282
	public void throwsExceptionWhenConditionKeywordIsUnsupported() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIsEmpty");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[0])));
	}

	@Test // gh-282
	public void throwsExceptionWhenInvalidNumberOfParameterIsGiven() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[0])));
	}

	@Test // gh-282
	public void createsQueryWithLimitToFindEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findTop3ByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 3");
	}

	@Test // gh-282
	public void createsQueryToFindFirstEntityByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findFirstByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // gh-341
	public void createsQueryToDeleteByFirstName() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("deleteByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);

		assertThat(bindableQuery.get()).isEqualTo("DELETE FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-344
	public void createsQueryToFindAllEntitiesByStringAttributeWithDistinct() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findDistinctByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "John" }));

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + DISTINCT + " " + TABLE + ".first_name, " + TABLE
				+ ".foo FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-363
	public void createsQueryForCountProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("countByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
			dataAccessStrategy);
		BindableQuery query = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[] { "John" })));

		assertThat(query.get())
			.isEqualTo("SELECT COUNT(users.id) FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	private R2dbcQueryMethod getQueryMethod(String methodName, Class<?>... parameterTypes) throws Exception {
		Method method = UserRepository.class.getMethod(methodName, parameterTypes);
		return new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory(), mappingContext);
	}

	private RelationalParametersParameterAccessor getAccessor(R2dbcQueryMethod queryMethod, Object[] values) {
		return new RelationalParametersParameterAccessor(queryMethod, values);
	}

	interface UserRepository extends Repository<User, Long> {

		Flux<User> findAllByFirstName(String firstName);

		Flux<User> findAllByLastNameAndFirstName(String lastName, String firstName);

		Flux<User> findAllByLastNameOrFirstName(String lastName, String firstName);

		Mono<Boolean> existsByFirstName(String firstName);

		Flux<User> findAllByDateOfBirthBetween(Date from, Date to);

		Flux<User> findAllByAgeLessThan(Integer age);

		Flux<User> findAllByAgeLessThanEqual(Integer age);

		Flux<User> findAllByAgeGreaterThan(Integer age);

		Flux<User> findAllByAgeGreaterThanEqual(Integer age);

		Flux<User> findAllByDateOfBirthAfter(Date date);

		Flux<User> findAllByDateOfBirthBefore(Date date);

		Flux<User> findAllByAgeIsNull();

		Flux<User> findAllByAgeIsNotNull();

		Flux<User> findAllByFirstNameLike(String like);

		Flux<User> findAllByFirstNameNotLike(String like);

		Flux<User> findAllByFirstNameStartingWith(String starting);

		Flux<User> findAllByFirstNameEndingWith(String ending);

		Flux<User> findAllByFirstNameContaining(String containing);

		Flux<User> findAllByFirstNameNotContaining(String notContaining);

		Flux<User> findAllByAgeOrderByLastNameAsc(Integer age);

		Flux<User> findAllByAgeOrderByLastNameDesc(Integer age);

		Flux<User> findAllByLastNameNot(String lastName);

		Flux<User> findAllByAgeIn(Collection<Integer> ages);

		Flux<User> findAllByAgeNotIn(Collection<Integer> ages);

		Flux<User> findAllByActiveTrue();

		Flux<User> findAllByActiveFalse();

		Flux<User> findAllByFirstNameIgnoreCase(String firstName);

		Mono<User> findByIdIgnoringCase(Long id);

		Flux<User> findAllByIdIn(Long id);

		Flux<User> findAllById(Collection<Long> ids);

		Flux<User> findAllByIdIsEmpty();

		Flux<User> findTop3ByFirstName(String firstName);

		Mono<User> findFirstByFirstName(String firstName);

		Mono<UserProjection> findDistinctByFirstName(String firstName);

		Mono<Integer> deleteByFirstName(String firstName);

		Mono<Long> countByFirstName(String firstName);
	}

	@Table("users")
	@Data
	private static class User {

		private @Id Long id;
		private String firstName;
		private String lastName;
		private Date dateOfBirth;
		private Integer age;
		private Boolean active;
	}

	interface UserProjection {

		String getFirstName();

		String getFoo();
	}
}
