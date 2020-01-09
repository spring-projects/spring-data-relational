/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Unit tests for {@link DefaultDataAccessStrategy}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Ignore
public class DefaultDataAccessStrategyUnitTests {

	public static final long ID_FROM_ADDITIONAL_VALUES = 23L;
	public static final long ORIGINAL_ID = 4711L;

	NamedParameterJdbcOperations namedJdbcOperations = mock(NamedParameterJdbcOperations.class);
	JdbcOperations jdbcOperations = mock(JdbcOperations.class);
	RelationalMappingContext context = new JdbcMappingContext();

	HashMap<SqlIdentifier, Object> additionalParameters = new HashMap<>();
	ArgumentCaptor<SqlParameterSource> paramSourceCaptor = ArgumentCaptor.forClass(SqlParameterSource.class);

	JdbcConverter converter;
	DefaultDataAccessStrategy accessStrategy;

	@Before
	public void before() {

		DelegatingDataAccessStrategy relationResolver = new DelegatingDataAccessStrategy();
		Dialect dialect = HsqlDbDialect.INSTANCE;

		converter = new BasicJdbcConverter(context, relationResolver, new JdbcCustomConversions(),
				new DefaultJdbcTypeFactory(jdbcOperations));
		accessStrategy = new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context, dialect), //
				context, //
				converter, //
				namedJdbcOperations);

		relationResolver.setDelegate(accessStrategy);
	}

	@Test // DATAJDBC-146
	public void additionalParameterForIdDoesNotLeadToDuplicateParameters() {

		additionalParameters.put(SqlIdentifier.quoted("ID"), ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(namedJdbcOperations).update(eq("INSERT INTO \"DUMMY_ENTITY\" (\"ID\") VALUES (:ID)"),
				paramSourceCaptor.capture(), any(KeyHolder.class));
	}

	@Test // DATAJDBC-146
	public void additionalParametersGetAddedToStatement() {

		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

		additionalParameters.put(unquoted("reference"), ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(namedJdbcOperations).update(sqlCaptor.capture(), paramSourceCaptor.capture(), any(KeyHolder.class));

		assertThat(sqlCaptor.getValue()) //
				.containsSequence("INSERT INTO \"DUMMY_ENTITY\" (", "\"ID\"", ") VALUES (", ":id", ")") //
				.containsSequence("INSERT INTO \"DUMMY_ENTITY\" (", "reference", ") VALUES (", ":reference", ")");
		assertThat(paramSourceCaptor.getValue().getValue("id")).isEqualTo(ORIGINAL_ID);
	}

	@Test // DATAJDBC-235
	public void considersConfiguredWriteConverter() {

		DelegatingDataAccessStrategy relationResolver = new DelegatingDataAccessStrategy();

		JdbcConverter converter = new BasicJdbcConverter(context, relationResolver,
				new JdbcCustomConversions(Arrays.asList(BooleanToStringConverter.INSTANCE, StringToBooleanConverter.INSTANCE)),
				new DefaultJdbcTypeFactory(jdbcOperations));

		DefaultDataAccessStrategy accessStrategy = new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context, HsqlDbDialect.INSTANCE), //
				context, //
				converter, //
				namedJdbcOperations);

		relationResolver.setDelegate(accessStrategy);

		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

		EntityWithBoolean entity = new EntityWithBoolean(ORIGINAL_ID, true);

		accessStrategy.insert(entity, EntityWithBoolean.class, new HashMap<>());

		verify(namedJdbcOperations).update(sqlCaptor.capture(), paramSourceCaptor.capture(), any(KeyHolder.class));

		assertThat(paramSourceCaptor.getValue().getValue("id")).isEqualTo(ORIGINAL_ID);
		assertThat(paramSourceCaptor.getValue().getValue("flag")).isEqualTo("T");
	}

	@RequiredArgsConstructor
	private static class DummyEntity {

		@Id private final Long id;
	}

	@AllArgsConstructor
	private static class EntityWithBoolean {

		@Id Long id;
		boolean flag;
	}

	@WritingConverter
	enum BooleanToStringConverter implements Converter<Boolean, String> {

		INSTANCE;

		@Override
		public String convert(Boolean source) {
			return source != null && source ? "T" : "F";
		}
	}

	@ReadingConverter
	enum StringToBooleanConverter implements Converter<String, Boolean> {

		INSTANCE;

		@Override
		public Boolean convert(String source) {
			return source != null && source.equalsIgnoreCase("T") ? Boolean.TRUE : Boolean.FALSE;
		}
	}

}
