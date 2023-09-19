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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link PartTreeJdbcQuery}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Diego Krupitza
 */
@ExtendWith(MockitoExtension.class)
public class PartTreeJdbcQueryUnitTests {

	private static final String TABLE = "\"users\"";
	private static final String ALL_FIELDS = "\"users\".\"ID\" AS \"ID\", \"users\".\"AGE\" AS \"AGE\", \"users\".\"ACTIVE\" AS \"ACTIVE\", \"users\".\"LAST_NAME\" AS \"LAST_NAME\", \"users\".\"FIRST_NAME\" AS \"FIRST_NAME\", \"users\".\"DATE_OF_BIRTH\" AS \"DATE_OF_BIRTH\", \"users\".\"HOBBY_REFERENCE\" AS \"HOBBY_REFERENCE\", \"hated\".\"NAME\" AS \"HATED_NAME\", \"users\".\"USER_CITY\" AS \"USER_CITY\", \"users\".\"USER_STREET\" AS \"USER_STREET\"";
	private static final String JOIN_CLAUSE = "FROM \"users\" LEFT OUTER JOIN \"HOBBY\" \"hated\" ON \"hated\".\"USERS\" = \"users\".\"ID\"";
	private static final String BASE_SELECT = "SELECT " + ALL_FIELDS + " " + JOIN_CLAUSE;

	JdbcMappingContext mappingContext = new JdbcMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(mappingContext, mock(RelationResolver.class));
	ReturnedType returnedType = mock(ReturnedType.class);

	@Test // DATAJDBC-318
	public void shouldFailForQueryByReference() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByHated", Hobby.class);
		assertThatIllegalArgumentException().isThrownBy(() -> createQuery(queryMethod));
	}

	@Test // GH-922
	public void createQueryByAggregateReference() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByHobbyReference", Hobby.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		Hobby hobby = new Hobby();
		hobby.name = "twentythree";
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { hobby }), returnedType);

		assertSoftly(softly -> {

			softly.assertThat(query.getQuery())
					.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"HOBBY_REFERENCE\" = :hobby_reference");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("hobby_reference")).isEqualTo("twentythree");
		});
	}

	@Test // GH-922
	void createQueryWithPessimisticWriteLock() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameAndLastName", String.class, String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);

		String firstname = "Diego";
		String lastname = "Krupitza";
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { firstname, lastname }),
				returnedType);

		assertSoftly(softly -> {

			softly.assertThat(query.getQuery().toUpperCase()).endsWith("FOR UPDATE");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo(firstname);
			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("last_name")).isEqualTo(lastname);
		});
	}

	@Test // GH-922
	void createQueryWithPessimisticReadLock() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameAndAge", String.class, Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);

		String firstname = "Diego";
		Integer age = 22;
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { firstname, age }),
				returnedType);

		assertSoftly(softly -> {

			// this is also for update since h2 dialect does not distinguish between lockmodes
			softly.assertThat(query.getQuery().toUpperCase()).endsWith("FOR UPDATE");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo(firstname);
			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("age")).isEqualTo(age);
		});
	}

	@Test // DATAJDBC-318
	public void shouldFailForQueryByList() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByHobbies", Object.class);
		assertThatIllegalArgumentException().isThrownBy(() -> createQuery(queryMethod));
	}

	@Test // DATAJDBC-318
	public void shouldFailForQueryByEmbeddedList() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findByAnotherEmbeddedList", Object.class);
		assertThatIllegalArgumentException().isThrownBy(() -> createQuery(queryMethod));
	}

	@Test // GH-922
	public void createQueryForQueryByAggregateReference() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findViaReferenceByHobbyReference", AggregateReference.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		AggregateReference<Object, String> hobby = AggregateReference.to("twentythree");
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { hobby }), returnedType);

		assertSoftly(softly -> {

			softly.assertThat(query.getQuery())
					.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"HOBBY_REFERENCE\" = :hobby_reference");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("hobby_reference")).isEqualTo("twentythree");
		});
	}

	@Test // GH-922
	public void createQueryForQueryByAggregateReferenceId() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findViaIdByHobbyReference", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		String hobby = "twentythree";
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { hobby }), returnedType);

		assertSoftly(softly -> {

			softly.assertThat(query.getQuery())
					.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"HOBBY_REFERENCE\" = :hobby_reference");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("hobby_reference")).isEqualTo("twentythree");
		});
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttribute() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "John" }), returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name");
	}

	@Test // GH-971
	public void createsQueryToFindAllEntitiesByProjectionAttribute() throws Exception {

		when(returnedType.needsCustomConstruction()).thenReturn(true);
		when(returnedType.getInputProperties()).thenReturn(Collections.singletonList("firstName"));

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "John" }), returnedType);

		assertThat(query.getQuery()).isEqualTo("SELECT " + TABLE + ".\"FIRST_NAME\" AS \"FIRST_NAME\" FROM \"users\""
				+ " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name");
	}

	@Test // DATAJDBC-318
	public void createsQueryWithIsNullCondition() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery((getAccessor(queryMethod, new Object[] { null })), returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" IS NULL");
	}

	@Test // DATAJDBC-318
	public void createsQueryWithLimitForExistsProjection() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery((getAccessor(queryMethod, new Object[] { "John" })), returnedType);

		assertThat(query.getQuery()).isEqualTo(
				"SELECT " + TABLE + ".\"ID\" FROM " + TABLE + " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name LIMIT 1");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByTwoStringAttributes() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameAndFirstName", String.class, String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "Doe", "John" }),
				returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"LAST_NAME\" = :last_name AND (" + TABLE
				+ ".\"FIRST_NAME\" = :first_name)");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByOneOfTwoStringAttributes() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameOrFirstName", String.class, String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { "Doe", "John" }),
				returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"LAST_NAME\" = :last_name OR (" + TABLE
				+ ".\"FIRST_NAME\" = :first_name)");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByDateAttributeBetween() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBetween", Date.class, Date.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		Date from = new Date();
		Date to = new Date();
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { from, to });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertSoftly(softly -> {

			softly.assertThat(query.getQuery())
					.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"DATE_OF_BIRTH\" BETWEEN :date_of_birth AND :date_of_birth1");

			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("date_of_birth")).isEqualTo(from);
			softly.assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("date_of_birth1")).isEqualTo(to);
		});
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeLessThan() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThan", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" < :age");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeLessThanEqual() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThanEqual", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" <= :age");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThan() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThan", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" > :age");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThanEqual() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThanEqual", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 30 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" >= :age");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByDateAttributeAfter() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthAfter", Date.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"DATE_OF_BIRTH\" > :date_of_birth");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByDateAttributeBefore() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBefore", Date.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { new Date() });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"DATE_OF_BIRTH\" < :date_of_birth");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeIsNull() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNull");
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" IS NULL");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeIsNotNull() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNotNull");
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" IS NOT NULL");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeLike() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameLike", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeNotLike() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotLike", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "%John%" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" NOT LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeStartingWith() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void appendsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Jo" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo("Jo%");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeEndingWith() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void prependsLikeOperatorParameterWithPercentSymbolForEndingWithQuery() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "hn" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo("%hn");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeContaining() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void wrapsLikeOperatorParameterWithPercentSymbolsForContainingQuery() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" LIKE :first_name");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo("%oh%");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeNotContaining() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" NOT LIKE :first_name");
	}

	@Test // DATAJDBC-318
	public void wrapsLikeOperatorParameterWithPercentSymbolsForNotContainingQuery() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotContaining", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "oh" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" NOT LIKE :first_name");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("first_name")).isEqualTo("%oh%");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeWithDescendingOrderingByStringAttribute()
			throws Exception {
		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameDesc", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 123 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery())
				.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" = :age ORDER BY \"users\".\"LAST_NAME\" DESC");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeWithAscendingOrderingByStringAttribute() throws Exception {
		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameAsc", Integer.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { 123 });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery())
				.isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" = :age ORDER BY \"users\".\"LAST_NAME\" ASC");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeNot() throws Exception {
		JdbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameNot", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Doe" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"LAST_NAME\" != :last_name");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeIn() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIn", Collection.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" IN (:age)");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByIntegerAttributeNotIn() throws Exception {
		JdbcQueryMethod queryMethod = getQueryMethod("findAllByAgeNotIn", Collection.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { Collections.singleton(25) });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"AGE\" NOT IN (:age)");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByBooleanAttributeTrue() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByActiveTrue");
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"ACTIVE\" = :active");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByBooleanAttributeFalse() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByActiveFalse");
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery()).isEqualTo(BASE_SELECT + " WHERE " + TABLE + ".\"ACTIVE\" = :active");
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindAllEntitiesByStringAttributeIgnoringCase() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameIgnoreCase", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		assertThat(query.getQuery())
				.isEqualTo(BASE_SELECT + " WHERE UPPER(" + TABLE + ".\"FIRST_NAME\") = UPPER(:first_name)");
	}

	@Test // DATAJDBC-318
	public void throwsExceptionWhenIgnoringCaseIsImpossible() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findByIdIgnoringCase", Long.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);

		assertThatIllegalStateException()
				.isThrownBy(() -> jdbcQuery.createQuery(getAccessor(queryMethod, new Object[] { 1L }), returnedType));
	}

	@Test // DATAJDBC-318
	public void throwsExceptionWhenConditionKeywordIsUnsupported() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByIdIsEmpty");
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> jdbcQuery.createQuery(getAccessor(queryMethod, new Object[0]), returnedType));
	}

	@Test // DATAJDBC-318
	public void throwsExceptionWhenInvalidNumberOfParameterIsGiven() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> jdbcQuery.createQuery(getAccessor(queryMethod, new Object[0]), returnedType));
	}

	@Test // DATAJDBC-318
	public void createsQueryWithLimitToFindEntitiesByStringAttribute() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findTop3ByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		String expectedSql = BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name LIMIT 3";
		assertThat(query.getQuery()).isEqualTo(expectedSql);
	}

	@Test // DATAJDBC-318
	public void createsQueryToFindFirstEntityByStringAttribute() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findFirstByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "John" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		String expectedSql = BASE_SELECT + " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name LIMIT 1";
		assertThat(query.getQuery()).isEqualTo(expectedSql);
	}

	@Test // DATAJDBC-318
	public void createsQueryByEmbeddedObject() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findByAddress", Address.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { new Address("Hello", "World") });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		String actualSql = query.getQuery();

		assertThat(actualSql) //
				.startsWith(BASE_SELECT + " WHERE (" + TABLE + ".\"USER_") //
				.endsWith(")") //
				.contains(TABLE + ".\"USER_STREET\" = :user_street", //
						" AND ", //
						TABLE + ".\"USER_CITY\" = :user_city");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("user_street")).isEqualTo("Hello");
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("user_city")).isEqualTo("World");
	}

	@Test // DATAJDBC-318
	public void createsQueryByEmbeddedProperty() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("findByAddressStreet", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[] { "Hello" });
		ParametrizedQuery query = jdbcQuery.createQuery(accessor, returnedType);

		String expectedSql = BASE_SELECT + " WHERE " + TABLE + ".\"USER_STREET\" = :user_street";

		assertThat(query.getQuery()).isEqualTo(expectedSql);
		assertThat(query.getParameterSource(Escaper.DEFAULT).getValue("user_street")).isEqualTo("Hello");
	}

	@Test // DATAJDBC-534
	public void createsQueryForCountProjection() throws Exception {

		JdbcQueryMethod queryMethod = getQueryMethod("countByFirstName", String.class);
		PartTreeJdbcQuery jdbcQuery = createQuery(queryMethod);
		ParametrizedQuery query = jdbcQuery.createQuery((getAccessor(queryMethod, new Object[] { "John" })), returnedType);

		assertThat(query.getQuery())
				.isEqualTo("SELECT COUNT(*) FROM " + TABLE + " WHERE " + TABLE + ".\"FIRST_NAME\" = :first_name");
	}

	private PartTreeJdbcQuery createQuery(JdbcQueryMethod queryMethod) {
		return new PartTreeJdbcQuery(mappingContext, queryMethod, H2Dialect.INSTANCE, converter,
				mock(NamedParameterJdbcOperations.class), mock(RowMapper.class));
	}

	private JdbcQueryMethod getQueryMethod(String methodName, Class<?>... parameterTypes) throws Exception {
		Method method = UserRepository.class.getMethod(methodName, parameterTypes);
		return new JdbcQueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory(), new PropertiesBasedNamedQueries(new Properties()), mappingContext);
	}

	private RelationalParametersParameterAccessor getAccessor(JdbcQueryMethod queryMethod, Object[] values) {
		return new RelationalParametersParameterAccessor(queryMethod, values);
	}

	@NoRepositoryBean
	interface UserRepository extends Repository<User, Long> {

		@Lock(LockMode.PESSIMISTIC_WRITE)
		List<User> findAllByFirstNameAndLastName(String firstName, String lastName);

		@Lock(LockMode.PESSIMISTIC_READ)
		List<User> findAllByFirstNameAndAge(String firstName, Integer age);

		List<User> findAllByFirstName(String firstName);

		List<User> findAllByHated(Hobby hobby);

		List<User> findAllByHatedName(String name);

		List<User> findAllByHobbies(Object hobbies);

		List<User> findAllByHobbyReference(Hobby hobby);

		List<User> findViaReferenceByHobbyReference(AggregateReference<Hobby, String> hobby);

		List<User> findViaIdByHobbyReference(String hobby);

		List<User> findAllByLastNameAndFirstName(String lastName, String firstName);

		List<User> findAllByLastNameOrFirstName(String lastName, String firstName);

		Boolean existsByFirstName(String firstName);

		List<User> findAllByDateOfBirthBetween(Date from, Date to);

		List<User> findAllByAgeLessThan(Integer age);

		List<User> findAllByAgeLessThanEqual(Integer age);

		List<User> findAllByAgeGreaterThan(Integer age);

		List<User> findAllByAgeGreaterThanEqual(Integer age);

		List<User> findAllByDateOfBirthAfter(Date date);

		List<User> findAllByDateOfBirthBefore(Date date);

		List<User> findAllByAgeIsNull();

		List<User> findAllByAgeIsNotNull();

		List<User> findAllByFirstNameLike(String like);

		List<User> findAllByFirstNameNotLike(String like);

		List<User> findAllByFirstNameStartingWith(String starting);

		List<User> findAllByFirstNameEndingWith(String ending);

		List<User> findAllByFirstNameContaining(String containing);

		List<User> findAllByFirstNameNotContaining(String notContaining);

		List<User> findAllByAgeOrderByLastNameAsc(Integer age);

		List<User> findAllByAgeOrderByLastNameDesc(Integer age);

		List<User> findAllByLastNameNot(String lastName);

		List<User> findAllByAgeIn(Collection<Integer> ages);

		List<User> findAllByAgeNotIn(Collection<Integer> ages);

		List<User> findAllByActiveTrue();

		List<User> findAllByActiveFalse();

		List<User> findAllByFirstNameIgnoreCase(String firstName);

		User findByIdIgnoringCase(Long id);

		List<User> findAllByIdIsEmpty();

		List<User> findTop3ByFirstName(String firstName);

		User findFirstByFirstName(String firstName);

		User findByAddress(Address address);

		User findByAddressStreet(String street);

		User findByAnotherEmbeddedList(Object list);

		long countByFirstName(String name);
	}

	@Table("users")
	static class User {

		@Id Long id;
		String firstName;
		String lastName;
		Date dateOfBirth;
		Integer age;
		Boolean active;

		@Embedded(prefix = "user_", onEmpty = Embedded.OnEmpty.USE_NULL) Address address;
		@Embedded.Nullable AnotherEmbedded anotherEmbedded;

		List<Hobby> hobbies;
		Hobby hated;

		AggregateReference<Hobby, String> hobbyReference;
	}

	record Address(String street, String city) {
	}

	record AnotherEmbedded(@MappedCollection(idColumn = "ID", keyColumn = "ORDER_KEY") List<Hobby> list) {
	}

	static class Hobby {
		@Id String name;
	}
}
