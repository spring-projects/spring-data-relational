/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Unit tests for {@link DefaultDataAccessStrategy}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class DefaultDataAccessStrategyUnitTests {

	public static final long ID_FROM_ADDITIONAL_VALUES = 23L;
	public static final long ORIGINAL_ID = 4711L;

	NamedParameterJdbcOperations jdbcOperations = mock(NamedParameterJdbcOperations.class);
	RelationalMappingContext context = new RelationalMappingContext();
	RelationalConverter converter = new BasicRelationalConverter(context, new JdbcCustomConversions());
	HashMap<String, Object> additionalParameters = new HashMap<>();
	ArgumentCaptor<SqlParameterSource> paramSourceCaptor = ArgumentCaptor.forClass(SqlParameterSource.class);

	DefaultDataAccessStrategy accessStrategy = new DefaultDataAccessStrategy( //
			new SqlGeneratorSource(context), //
			context, //
			converter, //
			jdbcOperations);

	@Test // DATAJDBC-146
	public void additionalParameterForIdDoesNotLeadToDuplicateParameters() {

		additionalParameters.put("id", ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(jdbcOperations).update(eq("INSERT INTO dummy_entity (id) VALUES (:id)"), paramSourceCaptor.capture(),
				any(KeyHolder.class));
	}

	@Test // DATAJDBC-146
	public void additionalParametersGetAddedToStatement() {

		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

		additionalParameters.put("reference", ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(jdbcOperations).update(sqlCaptor.capture(), paramSourceCaptor.capture(), any(KeyHolder.class));

		assertThat(sqlCaptor.getValue()) //
				.containsSequence("INSERT INTO dummy_entity (", "id", ") VALUES (", ":id", ")") //
				.containsSequence("INSERT INTO dummy_entity (", "reference", ") VALUES (", ":reference", ")");
		assertThat(paramSourceCaptor.getValue().getValue("id")).isEqualTo(ORIGINAL_ID);
	}

	@Test // DATAJDBC-235
	public void considersConfiguredWriteConverter() {

		RelationalConverter converter = new BasicRelationalConverter(context,
				new JdbcCustomConversions(Arrays.asList(BooleanToStringConverter.INSTANCE, StringToBooleanConverter.INSTANCE)));

		DefaultDataAccessStrategy accessStrategy = new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context), //
				context, //
				converter, //
				jdbcOperations);

		ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

		EntityWithBoolean entity = new EntityWithBoolean(ORIGINAL_ID, true);

		accessStrategy.insert(entity, EntityWithBoolean.class, new HashMap<>());

		verify(jdbcOperations).update(sqlCaptor.capture(), paramSourceCaptor.capture(), any(KeyHolder.class));

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
