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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.annotation.Id;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
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
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Unit tests for {@link PartTreeR2dbcQuery}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @author Myeonghyeon Lee
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PartTreeR2dbcQueryUnitTests {

	private static final String TABLE = "users";
	private static final String ALL_FIELDS = TABLE + ".id, " + TABLE + ".first_name, " + TABLE + ".last_name, " + TABLE
			+ ".date_of_birth, " + TABLE + ".age, " + TABLE + ".active";
	private static final String DISTINCT = "DISTINCT";

	@Mock ConnectionFactory connectionFactory;
	@Mock R2dbcConverter r2dbcConverter;

	private RelationalMappingContext mappingContext;
	private ReactiveDataAccessStrategy dataAccessStrategy;
	private DatabaseClient databaseClient;

	@BeforeEach
	void setUp() {

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
	void createsQueryToFindAllEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = createQuery(queryMethod, r2dbcQuery, "John");

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-282
	void createsQueryWithIsNullCondition() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = createQuery(queryMethod, r2dbcQuery, new Object[] { null });

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name IS NULL");
	}

	@Test // gh-282
	void createsQueryWithLimitForExistsProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery query = createQuery(queryMethod, r2dbcQuery, "John");

		assertThat(query.get())
				.isEqualTo("SELECT " + TABLE + ".id FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameAndFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE
				+ ".last_name = $1 AND (" + TABLE + ".first_name = $2)");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByOneOfTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameOrFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE
				+ ".last_name = $1 OR (" + TABLE + ".first_name = $2)");
	}

	@Test // gh-282, gh-349
	void createsQueryToFindAllEntitiesByDateAttributeBetween() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBetween", Date.class, Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		Date from = new Date();
		Date to = new Date();
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { from, to });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth BETWEEN $1 AND $2");

		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		when(bindSpecMock.bind(anyInt(), any())).thenReturn(bindSpecMock);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, from);
		verify(bindSpecMock, times(1)).bind(1, to);
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeLessThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age < $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeLessThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age <= $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age > $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age >= $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByDateAttributeAfter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthAfter", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth > $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByDateAttributeBefore() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBefore", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth < $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIsNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NULL");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIsNotNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNotNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NOT NULL");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeNotLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name NOT LIKE $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeStartingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	void appendsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "Jo%");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeEndingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	void prependsLikeOperatorParameterWithPercentSymbolForEndingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%hn");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	void wrapsLikeOperatorParameterWithPercentSymbolsForContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%oh%");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeNotContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name NOT LIKE $1");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test // gh-282
	void wrapsLikeOperatorParameterWithPercentSymbolsForNotContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);
		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);
		bindableQuery.bind(bindSpecMock);

		verify(bindSpecMock, times(1)).bind(0, "%oh%");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeWithDescendingOrderingByStringAttribute()
			throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameDesc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age = $1 ORDER BY last_name DESC");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeWithAscendingOrderingByStringAttribute() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameAsc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age = $1 ORDER BY last_name ASC");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeNot() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameNot", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Doe" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".last_name != $1");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIn() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IN ($1)");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByIntegerAttributeNotIn() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeNotIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age NOT IN ($1)");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByBooleanAttributeTrue() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveTrue");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = TRUE");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByBooleanAttributeFalse() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveFalse");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = FALSE");
	}

	@Test // gh-282
	void createsQueryToFindAllEntitiesByStringAttributeIgnoringCase() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameIgnoreCase", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE UPPER(" + TABLE + ".first_name) = UPPER($1)");
	}

	@Test // gh-282
	void throwsExceptionWhenIgnoringCaseIsImpossible() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findByIdIgnoringCase", Long.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalStateException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[] { 1L })));
	}

	@Test // gh-282
	void throwsExceptionWhenInPredicateHasNonIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIn", Long.class);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter, dataAccessStrategy));
	}

	@Test // gh-282
	void throwsExceptionWhenSimplePropertyPredicateHasIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllById", Collection.class);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter, dataAccessStrategy));
	}

	@Test // gh-282
	void throwsExceptionWhenConditionKeywordIsUnsupported() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIsEmpty");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[0])));
	}

	@Test // gh-282
	void throwsExceptionWhenInvalidNumberOfParameterIsGiven() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[0])));
	}

	@Test // gh-282
	void createsQueryWithLimitToFindEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findTop3ByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 3");
	}

	@Test // gh-282
	void createsQueryToFindFirstEntityByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findFirstByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get())
				.isEqualTo("SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // gh-341
	void createsQueryToDeleteByFirstName() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("deleteByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		BindableQuery bindableQuery = createQuery(r2dbcQuery, accessor);

		assertThat(bindableQuery.get()).isEqualTo("DELETE FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-344
	void createsQueryToFindAllEntitiesByStringAttributeWithDistinct() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findDistinctByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
				dataAccessStrategy);
		BindableQuery bindableQuery = createQuery(queryMethod, r2dbcQuery, "John");

		assertThat(bindableQuery.get()).isEqualTo("SELECT " + DISTINCT + " " + TABLE + ".first_name, " + TABLE
				+ ".foo FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	@Test // gh-363
	void createsQueryForCountProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("countByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
			dataAccessStrategy);
		BindableQuery query = createQuery(queryMethod, r2dbcQuery, "John");

		assertThat(query.get())
			.isEqualTo("SELECT COUNT(users.id) FROM " + TABLE + " WHERE " + TABLE + ".first_name = $1");
	}

	private BindableQuery createQuery(R2dbcQueryMethod queryMethod, PartTreeR2dbcQuery r2dbcQuery, Object... parameters) {
		return createQuery(r2dbcQuery, getAccessor(queryMethod, parameters));
	}

	private BindableQuery createQuery(PartTreeR2dbcQuery r2dbcQuery, RelationalParametersParameterAccessor accessor) {
		return r2dbcQuery.createQuery(accessor).block();
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
