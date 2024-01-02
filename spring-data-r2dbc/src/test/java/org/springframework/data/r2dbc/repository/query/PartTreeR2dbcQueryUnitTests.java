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
package org.springframework.data.r2dbc.repository.query;

import static org.mockito.Mockito.*;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.domain.SqlSort;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * Unit tests for {@link PartTreeR2dbcQuery}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @author Myeonghyeon Lee
 * @author Diego Krupitza
 * @author Philmon Roberts
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PartTreeR2dbcQueryUnitTests {

	private static final String TABLE = "users";
	private static final String[] ALL_FIELDS_ARRAY = new String[] { ".id", ".first_name", ".last_name", ".date_of_birth",
			".age", ".active" };
	private static final String[] ALL_FIELDS_ARRAY_PREFIXED = Arrays.stream(ALL_FIELDS_ARRAY).map(f -> TABLE + f)
			.toArray(String[]::new);
	private static final String ALL_FIELDS = String.join(", ", ALL_FIELDS_ARRAY_PREFIXED);
	private static final String DISTINCT = "DISTINCT";

	@Mock ConnectionFactory connectionFactory;
	@Mock R2dbcConverter r2dbcConverter;

	private RelationalMappingContext mappingContext;
	private ReactiveDataAccessStrategy dataAccessStrategy;
	private R2dbcEntityOperations operations;

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

		operations = new R2dbcEntityTemplate(DatabaseClient.builder().connectionFactory(connectionFactory).build(),
				dataAccessStrategy);
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1");
	}

	@Test // GH-282
	void createsQueryWithIsNullCondition() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery, new Object[] { null });

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name IS NULL");
	}

	@Test // GH-282
	void createsQueryWithLimitForExistsProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(query) //
				.selects(TABLE + ".id") //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameAndFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery,
				getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".last_name = $1 AND (" + TABLE + ".first_name = $2)");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByOneOfTwoStringAttributes() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameOrFirstName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery,
				getAccessor(queryMethod, new Object[] { "Doe", "John" }));

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".last_name = $1 OR (" + TABLE + ".first_name = $2)");
	}

	@Test // GH-282, GH-349
	void createsQueryToFindAllEntitiesByDateAttributeBetween() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBetween", Date.class, Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		Date from = new Date();
		Date to = new Date();
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { from, to });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".date_of_birth BETWEEN $1 AND $2");

		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, from);
		verify(bindTarget, times(1)).bind(1, to);
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeLessThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age < $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeLessThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age <= $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThan() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThan", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age > $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThanEqual() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThanEqual", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age >= $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByDateAttributeAfter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthAfter", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".date_of_birth > $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByDateAttributeBefore() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBefore", Date.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".date_of_birth < $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIsNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age IS NULL");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIsNotNull() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNotNull");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age IS NOT NULL");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name LIKE $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeNotLike() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotLike", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name NOT LIKE $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeStartingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name LIKE $1");
	}

	@Test // GH-282
	void appendsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);
		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, "Jo%");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeEndingWith() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name LIKE $1");
	}

	@Test // GH-282
	void prependsLikeOperatorParameterWithPercentSymbolForEndingWithQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);
		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, "%hn");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name LIKE $1");
	}

	@Test // GH-282
	void wrapsLikeOperatorParameterWithPercentSymbolsForContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);
		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, "%oh%");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeNotContaining() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name NOT LIKE $1");
	}

	@Test // GH-282
	void wrapsLikeOperatorParameterWithPercentSymbolsForNotContainingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);
		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, "%oh%");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeWithDescendingOrderingByStringAttribute() throws Exception {
		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameDesc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age = $1") //
				.orderBy("users.last_name DESC");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeWithAscendingOrderingByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameAsc", Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age = $1") //
				.orderBy("users.last_name ASC");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeNot() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameNot", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Doe" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".last_name != $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeIn() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age IN ($1)");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByIntegerAttributeNotIn() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeNotIn", Collection.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".age NOT IN ($1)");
	}

	@Test // GH-282, GH-698
	void createsQueryToFindAllEntitiesByBooleanAttributeTrue() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveTrue");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".active = $1");
	}

	@Test // GH-282, GH-698
	void createsQueryToFindAllEntitiesByBooleanAttributeFalse() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveFalse");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".active = $1");
	}

	@Test // GH-282
	void createsQueryToFindAllEntitiesByStringAttributeIgnoringCase() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameIgnoreCase", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where("UPPER(" + TABLE + ".first_name) = UPPER($1)");
	}

	@Test // GH-282
	void throwsExceptionWhenIgnoringCaseIsImpossible() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findByIdIgnoringCase", Long.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		Assertions.assertThatIllegalStateException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[] { 1L })));
	}

	@Test // GH-282
	void throwsExceptionWhenInPredicateHasNonIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIn", Long.class);

		Assertions.assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy));
	}

	@Test // GH-282
	void throwsExceptionWhenSimplePropertyPredicateHasIterableParameter() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllById", Collection.class);

		Assertions.assertThatIllegalArgumentException()
				.isThrownBy(() -> new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy));
	}

	@Test // GH-282
	void throwsExceptionWhenConditionKeywordIsUnsupported() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByIdIsEmpty");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		Assertions.assertThatIllegalArgumentException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[0])));
	}

	@Test // GH-1548
	void allowsSortingByNonDomainProperties() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class, Sort.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery, "foo", Sort.by("foobar"));
		PreparedOperationAssert.assertThat(preparedOperation) //
				.orderBy("users.foobar ASC");

		preparedOperation = createQuery(queryMethod, r2dbcQuery, "foo", SqlSort.unsafe(Direction.ASC, "sum(foobar)"));
		PreparedOperationAssert.assertThat(preparedOperation) //
				.orderBy("sum(foobar) ASC");
	}

	@Test // GH-282
	void throwsExceptionWhenInvalidNumberOfParameterIsGiven() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		Assertions.assertThatIllegalArgumentException()
				.isThrownBy(() -> createQuery(r2dbcQuery, getAccessor(queryMethod, new Object[0])));
	}

	@Test // GH-282
	void createsQueryWithLimitToFindEntitiesByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findTop3ByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1 LIMIT 3");
	}

	@Test // GH-282
	void createsQueryToFindFirstEntityByStringAttribute() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findFirstByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects(ALL_FIELDS_ARRAY_PREFIXED) //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1 LIMIT 1");
	}

	@Test // GH-341
	void createsQueryToDeleteByFirstName() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("deleteByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.deletes() //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1");
	}

	@Test // GH-344
	void createsQueryToFindAllEntitiesByStringAttributeWithDistinct() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findDistinctByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selectsDistinct(TABLE + ".first_name", TABLE + ".foo") //
				.from(TABLE);

	}

	@Test // GH-475
	void createsQueryToFindByOpenProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findOpenProjectionBy");
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects("users.id", "users.first_name", "users.last_name", "users.date_of_birth", "users.age", "users.active") //
				.from(TABLE);
	}

	@Test
		// GH-475, GH-1687
	void createsDtoProjectionQuery() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAsDtoProjectionByAge", Integer.TYPE);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> preparedOperation = createQuery(queryMethod, r2dbcQuery, 42);

		PreparedOperationAssert.assertThat(preparedOperation) //
				.selects("users.id", "users.first_name", "users.last_name", "users.date_of_birth", "users.age", "users.active") //
				.from(TABLE);
	}

	@Test // GH-363
	void createsQueryForCountProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("countByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(query) //
				.selects("COUNT(users.id)") //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1");
	}

	@Test // GH-1041
	void createQueryWithPessimisticWriteLock() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameAndLastName", String.class, String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		String firstname = "Diego";
		String lastname = "Krupitza";

		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, firstname, lastname);

		PreparedOperationAssert.assertThat(query) //
				.selects("users.id", "users.first_name", "users.last_name", "users.date_of_birth", "users.age", "users.active") //
				.from(TABLE) //
				.where("users.first_name = $1 AND (users.last_name = $2) FOR UPDATE OF users");
	}

	@Test // GH-1041
	void createQueryWithPessimisticReadLock() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameAndAge", String.class, Integer.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);

		String firstname = "Diego";
		Integer age = 22;

		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, firstname, age);

		PreparedOperationAssert.assertThat(query) //
				.selects("users.id", "users.first_name", "users.last_name", "users.date_of_birth", "users.age", "users.active") //
				.from(TABLE) //
				.where("users.first_name = $1 AND (users.age = $2) FOR SHARE OF users");
	}

	@Test // GH-1285
	void bindsParametersFromPublisher() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod("findByFirstName", Mono.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(queryMethod, Mono.just("John"));

		PreparedOperation<?> preparedOperation = createQuery(r2dbcQuery, accessor.resolveParameters().block());
		BindTarget bindTarget = mock(BindTarget.class);
		preparedOperation.bindTo(bindTarget);

		verify(bindTarget, times(1)).bind(0, "John");
	}

	@Test // GH-1310
	void createsQueryWithoutIdForCountProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod(WithoutIdRepository.class, "countByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(query) //
				.selects("COUNT(1)") //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1");
	}

	@Test // GH-1310
	void createsQueryWithoutIdForExistsProjection() throws Exception {

		R2dbcQueryMethod queryMethod = getQueryMethod(WithoutIdRepository.class, "existsByFirstName", String.class);
		PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, operations, r2dbcConverter, dataAccessStrategy);
		PreparedOperation<?> query = createQuery(queryMethod, r2dbcQuery, "John");

		PreparedOperationAssert.assertThat(query) //
				.selects("1") //
				.from(TABLE) //
				.where(TABLE + ".first_name = $1 LIMIT 1");
	}

	private PreparedOperation<?> createQuery(R2dbcQueryMethod queryMethod, PartTreeR2dbcQuery r2dbcQuery,
			Object... parameters) {
		return createQuery(r2dbcQuery, getAccessor(queryMethod, parameters));
	}

	private PreparedOperation<?> createQuery(PartTreeR2dbcQuery r2dbcQuery,
			RelationalParametersParameterAccessor accessor) {
		return r2dbcQuery.createQuery(accessor).block();
	}

	private R2dbcQueryMethod getQueryMethod(String methodName, Class<?>... parameterTypes) throws Exception {
		return getQueryMethod(UserRepository.class, methodName, parameterTypes);
	}

	private R2dbcQueryMethod getQueryMethod(Class<?> repository, String methodName, Class<?>... parameterTypes)
			throws Exception {
		Method method = repository.getMethod(methodName, parameterTypes);
		return new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(repository),
				new SpelAwareProxyProjectionFactory(), mappingContext);
	}

	private RelationalParametersParameterAccessor getAccessor(R2dbcQueryMethod queryMethod, Object[] values) {
		return new RelationalParametersParameterAccessor(queryMethod, values);
	}

	private static class PreparedOperationAssert extends AbstractAssert<PreparedOperationAssert, PreparedOperation<?>> {

		private final String query;
		private final String[] columns;
		private final String table;
		private final String whereClause;
		private final String orderClause;
		private final boolean isDistinct;

		public static PreparedOperationAssert assertThat(PreparedOperation<?> preparedOperation) {
			return new PreparedOperationAssert(preparedOperation);
		}

		PreparedOperationAssert(PreparedOperation preparedOperation) {

			super(preparedOperation, PreparedOperationAssert.class);

			query = preparedOperation.get();

			String select = "SELECT";
			String distinct = "DISTINCT";
			String from = "FROM";
			String where = "WHERE";
			String orderBy = "ORDER BY";

			int indexOfSelect = query.toUpperCase().indexOf(select);
			int indexOfFrom = query.toUpperCase().indexOf(from);
			int indexOfWhere = query.toUpperCase().indexOf(where);
			int indexOfOrderBy = query.toUpperCase().indexOf(orderBy);

			final int endOfFrom = indexOfWhere < 0 ? query.length() : indexOfWhere;
			final int endOfWhereClause = indexOfOrderBy < 0 ? query.length() : indexOfOrderBy;

			String columnsString = query.substring(select.length(), indexOfFrom).trim();

			isDistinct = columnsString.startsWith(distinct);
			if (isDistinct) {
				columnsString = columnsString.substring(distinct.length()).trim();
			}

			columns = columnsString.split(",? ");
			table = query.substring(indexOfFrom + from.length(), endOfFrom).trim();
			whereClause = query.substring(indexOfWhere + where.length(), endOfWhereClause).trim();
			orderClause = query.substring(indexOfOrderBy + orderBy.length()).trim();
		}

		PreparedOperationAssert selects(String... columns) {

			Assertions.assertThat(this.columns).containsExactlyInAnyOrder(columns);
			if (isDistinct) {
				Assertions.fail(query + " is not expected to be distinct, but is.");
			}
			return this;
		}

		PreparedOperationAssert selectsDistinct(String... columns) {

			Assertions.assertThat(this.columns).containsExactlyInAnyOrder(columns);
			if (!isDistinct) {
				Assertions.fail(query + " is expected to be distinct, but is not.");
			}
			return this;
		}

		PreparedOperationAssert from(String table) {

			Assertions.assertThat(this.table).isEqualTo(table);
			return this;
		}

		PreparedOperationAssert where(String condition) {

			Assertions.assertThat(this.whereClause).isEqualTo(condition);
			return this;
		}

		PreparedOperationAssert orderBy(String orderBy) {

			Assertions.assertThat(this.orderClause).isEqualTo(orderBy);
			return this;
		}

		PreparedOperationAssert deletes() {

			Assertions.assertThat(query).startsWith("DELETE FROM");
			return this;
		}
	}

	@SuppressWarnings("ALL")
	interface UserRepository extends Repository<User, Long> {

		@Lock(LockMode.PESSIMISTIC_WRITE)
		Flux<User> findAllByFirstNameAndLastName(String firstName, String lastName);

		@Lock(LockMode.PESSIMISTIC_READ)
		Flux<User> findAllByFirstNameAndAge(String firstName, Integer age);

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

		Flux<User> findAllByFirstName(String firstName, Sort sort);

		Flux<User> findTop3ByFirstName(String firstName);

		Mono<User> findFirstByFirstName(String firstName);

		Mono<UserProjection> findDistinctByFirstName(String firstName);

		Mono<OpenUserProjection> findOpenProjectionBy();

		Mono<UserDtoProjection> findAsDtoProjectionByAge(int age);

		Mono<Integer> deleteByFirstName(String firstName);

		Mono<Long> countByFirstName(String firstName);

		Mono<User> findByFirstName(Mono<String> firstName);

	}

	interface WithoutIdRepository extends Repository<WithoutId, Long> {

		Mono<Boolean> existsByFirstName(String firstName);

		Mono<Long> countByFirstName(String firstName);
	}

	@Table("users")
	private static class User {

		private @Id Long id;
		private String firstName;
		private String lastName;
		private Date dateOfBirth;
		private Integer age;
		private Boolean active;
	}

	@Table("users")
	private static class WithoutId {

		private String firstName;
	}

	interface UserProjection {

		String getFirstName();

		String getFoo();
	}

	interface OpenUserProjection {

		String getFirstName();

		@Value("#firstName")
		String getFoo();
	}

	static class UserDtoProjection {

		String firstName;
		String unknown;
	}
}
