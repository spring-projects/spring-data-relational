/*
 * Copyright 2022-2023 the original author or authors.
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

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategyUnitTests.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Unit tests for {@link SqlParametersFactory}.
 *
 * @author Chirag Tailor
 */
class SqlParametersFactoryTest {

	RelationalMappingContext context = new JdbcMappingContext();
	RelationResolver relationResolver = mock(RelationResolver.class);
	BasicJdbcConverter converter = new BasicJdbcConverter(context, relationResolver);
	AnsiDialect dialect = AnsiDialect.INSTANCE;
	SqlParametersFactory sqlParametersFactory = new SqlParametersFactory(context, converter, dialect);

	@Test // DATAJDBC-412
	public void considersConfiguredWriteConverterForIdValueObjects_onRead() {

		SqlParametersFactory sqlParametersFactory = createSqlParametersFactoryWithConverters(
				singletonList(IdValueToStringConverter.INSTANCE));

		String rawId = "batman";
		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory.forQueryById(new IdValue(rawId),
				WithValueObjectId.class, SqlGenerator.ID_SQL_PARAMETER);

		assertThat(sqlParameterSource.getValue("id")).isEqualTo(rawId);
	}

	@Test // DATAJDBC-349
	public void considersConfiguredWriteConverterForIdValueObjectsWhichReferencedInOneToManyRelationship() {

		SqlParametersFactory sqlParametersFactory = createSqlParametersFactoryWithConverters(
				singletonList(IdValueToStringConverter.INSTANCE));

		String rawId = "batman";
		IdValue rootIdValue = new IdValue(rawId);

		DummyEntityRoot root = new DummyEntityRoot(rootIdValue);
		DummyEntity child = new DummyEntity(ORIGINAL_ID);
		root.dummyEntities.add(child);

		HashMap<SqlIdentifier, Object> additionalParameters = new HashMap<>();
		additionalParameters.put(SqlIdentifier.quoted("DUMMYENTITYROOT"), rootIdValue);

		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory
				.forQueryByIdentifier(Identifier.from(additionalParameters));

		assertThat(sqlParameterSource.getValue("DUMMYENTITYROOT")).isEqualTo(rawId);
	}

	@Test // DATAJDBC-146
	void identifiersGetAddedAsParameters() {

		long id = 4711L;
		DummyEntity instance = new DummyEntity(id);
		long reference = 23L;
		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory.forInsert(instance, DummyEntity.class,
				Identifier.of(SqlIdentifier.unquoted("reference"), reference, Long.class), IdValueSource.PROVIDED);

		assertThat(sqlParameterSource.getParameterNames()).hasSize(2);
		assertThat(sqlParameterSource.getValue("id")).isEqualTo(id);
		assertThat(sqlParameterSource.getValue("reference")).isEqualTo(reference);
	}

	@Test // DATAJDBC-146
	void additionalIdentifierForIdDoesNotLeadToDuplicateParameters() {

		long id = 4711L;
		DummyEntity instance = new DummyEntity(id);
		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory.forInsert(instance, DummyEntity.class,
				Identifier.of(SqlIdentifier.unquoted("id"), 23L, Long.class), IdValueSource.PROVIDED);

		assertThat(sqlParameterSource.getParameterNames()).hasSize(1);
		assertThat(sqlParameterSource.getValue("id")).isEqualTo(id);
	}

	@Test // DATAJDBC-235
	void considersConfiguredWriteConverter() {

		SqlParametersFactory sqlParametersFactory = createSqlParametersFactoryWithConverters(
				asList(BooleanToStringConverter.INSTANCE, StringToBooleanConverter.INSTANCE));

		long id = 4711L;
		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory.forInsert(new EntityWithBoolean(id, true),
				EntityWithBoolean.class, Identifier.empty(), IdValueSource.PROVIDED);

		assertThat(sqlParameterSource.getValue("id")).isEqualTo(id);
		assertThat(sqlParameterSource.getValue("flag")).isEqualTo("T");
	}

	@Test // DATAJDBC-412
	void considersConfiguredWriteConverterForIdValueObjects_onWrite() {

		SqlParametersFactory sqlParametersFactory = createSqlParametersFactoryWithConverters(
				singletonList(IdValueToStringConverter.INSTANCE));

		String rawId = "batman";
		WithValueObjectId entity = new WithValueObjectId(new IdValue(rawId));
		String value = "vs. superman";
		entity.value = value;

		SqlIdentifierParameterSource sqlParameterSource = sqlParametersFactory.forInsert(entity, WithValueObjectId.class,
				Identifier.empty(), IdValueSource.PROVIDED);
		assertThat(sqlParameterSource.getValue("id")).isEqualTo(rawId);
		assertThat(sqlParameterSource.getValue("value")).isEqualTo(value);
	}

	@WritingConverter
	enum IdValueToStringConverter implements Converter<IdValue, String> {

		INSTANCE;

		@Override
		public String convert(IdValue source) {
			return source.id;
		}
	}

	@Data
	private static class WithValueObjectId {

		@Id private final IdValue id;
		String value;
	}

	@Value
	private static class IdValue {
		String id;
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

	@AllArgsConstructor
	private static class EntityWithBoolean {

		@Id Long id;
		boolean flag;
	}

	@RequiredArgsConstructor // DATAJDBC-349
	private static class DummyEntityRoot {

		@Id private final IdValue id;
		List<DummyEntity> dummyEntities = new ArrayList<>();
	}

	@RequiredArgsConstructor
	private static class DummyEntity {

		@Id private final Long id;
	}

	private SqlParametersFactory createSqlParametersFactoryWithConverters(List<?> converters) {

		BasicJdbcConverter converter = new BasicJdbcConverter(context, relationResolver,
				new JdbcCustomConversions(converters), new DefaultJdbcTypeFactory(mock(JdbcOperations.class)),
				dialect.getIdentifierProcessing());
		return new SqlParametersFactory(context, converter, dialect);
	}
}
