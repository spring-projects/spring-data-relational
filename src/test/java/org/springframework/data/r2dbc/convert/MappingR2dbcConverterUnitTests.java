/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Row;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * Unit tests for {@link MappingR2dbcConverter}.
 *
 * @author Mark Paluch
 */
public class MappingR2dbcConverterUnitTests {

	RelationalMappingContext mappingContext = new R2dbcMappingContext();
	MappingR2dbcConverter converter = new MappingR2dbcConverter(mappingContext);

	@Before
	public void before() {

		R2dbcCustomConversions conversions = new R2dbcCustomConversions(
				Arrays.asList(StringToMapConverter.INSTANCE, MapToStringConverter.INSTANCE,
						CustomConversionPersonToOutboundRowConverter.INSTANCE, RowToCustomConversionPerson.INSTANCE));

		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		converter = new MappingR2dbcConverter(mappingContext, conversions);
	}

	@Test // gh-61, gh-207
	public void shouldIncludeAllPropertiesInOutboundRow() {

		OutboundRow row = new OutboundRow();

		Instant instant = Instant.now();
		LocalDateTime localDateTime = LocalDateTime.now();
		converter.write(new Person("id", "Walter", "White", instant, localDateTime), row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("id"), Parameter.fromOrEmpty("id", String.class));
		assertThat(row).containsEntry(SqlIdentifier.unquoted("firstname"),
				Parameter.fromOrEmpty("Walter", String.class));
		assertThat(row).containsEntry(SqlIdentifier.unquoted("lastname"), Parameter.fromOrEmpty("White", String.class));
		assertThat(row).containsEntry(SqlIdentifier.unquoted("instant"), Parameter.from(instant));
		assertThat(row).containsEntry(SqlIdentifier.unquoted("local_date_time"), Parameter.from(localDateTime));
	}

	@Test // gh-41
	public void shouldPassThroughRow() {

		Row rowMock = mock(Row.class);

		Row result = converter.read(Row.class, rowMock);

		assertThat(result).isSameAs(rowMock);
	}

	@Test // gh-41
	public void shouldConvertRowToNumber() {

		Row rowMock = mock(Row.class);
		when(rowMock.get(0)).thenReturn(42);

		Integer result = converter.read(Integer.class, rowMock);

		assertThat(result).isEqualTo(42);
	}

	@Test // gh-59
	public void shouldFailOnUnsupportedEntity() {

		PersonWithConversions withMap = new PersonWithConversions(null, null, new NonMappableEntity());
		OutboundRow row = new OutboundRow();

		assertThatThrownBy(() -> converter.write(withMap, row)).isInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Test // gh-59
	public void shouldConvertMapToString() {

		PersonWithConversions withMap = new PersonWithConversions("foo", Collections.singletonMap("map", "value"), null);
		OutboundRow row = new OutboundRow();
		converter.write(withMap, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("nested"), Parameter.from("map"));
	}

	@Test // gh-59
	public void shouldReadMapFromString() {

		Row rowMock = mock(Row.class);
		when(rowMock.get("nested")).thenReturn("map");

		PersonWithConversions result = converter.read(PersonWithConversions.class, rowMock);

		assertThat(result.nested).isEqualTo(Collections.singletonMap("map", "map"));
	}

	@Test // gh-59
	public void shouldConvertEnum() {

		WithEnum withMap = new WithEnum("foo", Condition.Mint);
		OutboundRow row = new OutboundRow();
		converter.write(withMap, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("condition"), Parameter.from("Mint"));
	}

	@Test // gh-59
	public void shouldConvertNullEnum() {

		WithEnum withMap = new WithEnum("foo", null);
		OutboundRow row = new OutboundRow();
		converter.write(withMap, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("condition"), Parameter.fromOrEmpty(null, String.class));
	}

	@Test // gh-59
	public void shouldReadEnum() {

		Row rowMock = mock(Row.class);
		when(rowMock.get("condition")).thenReturn("Mint");

		WithEnum result = converter.read(WithEnum.class, rowMock);

		assertThat(result.condition).isEqualTo(Condition.Mint);
	}

	@Test // gh-59
	public void shouldWriteTopLevelEntity() {

		CustomConversionPerson person = new CustomConversionPerson();
		person.entity = new NonMappableEntity();
		person.foo = "bar";

		OutboundRow row = new OutboundRow();
		converter.write(person, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("foo_column"), Parameter.from("bar"))
				.containsEntry(SqlIdentifier.unquoted("entity"), Parameter.from("nested_entity"));
	}

	@Test // gh-59
	public void shouldReadTopLevelEntity() {

		Row rowMock = mock(Row.class);
		when(rowMock.get("foo_column", String.class)).thenReturn("bar");
		when(rowMock.get("nested_entity")).thenReturn("map");

		CustomConversionPerson result = converter.read(CustomConversionPerson.class, rowMock);

		assertThat(result.foo).isEqualTo("bar");
		assertThat(result.entity).isNotNull();
	}

	@AllArgsConstructor
	static class Person {
		@Id String id;
		String firstname, lastname;
		Instant instant;
		LocalDateTime localDateTime;
	}

	@AllArgsConstructor
	static class WithEnum {
		@Id String id;
		Condition condition;
	}

	enum Condition {
		Mint, Used
	}

	@AllArgsConstructor
	static class PersonWithConversions {
		@Id String id;
		Map<String, String> nested;
		NonMappableEntity unsupported;
	}

	static class CustomConversionPerson {

		String foo;
		NonMappableEntity entity;
	}

	static class NonMappableEntity {}

	@ReadingConverter
	enum StringToMapConverter implements Converter<String, Map<String, String>> {

		INSTANCE;

		@Override
		public Map<String, String> convert(String source) {

			if (source != null) {
				return Collections.singletonMap(source, source);
			}

			return null;
		}
	}

	@WritingConverter
	enum MapToStringConverter implements Converter<Map<String, String>, String> {

		INSTANCE;

		@Override
		public String convert(Map<String, String> source) {

			if (!source.isEmpty()) {
				return source.keySet().iterator().next();
			}

			return null;
		}
	}

	@WritingConverter
	enum CustomConversionPersonToOutboundRowConverter implements Converter<CustomConversionPerson, OutboundRow> {

		INSTANCE;

		@Override
		public OutboundRow convert(CustomConversionPerson source) {

			OutboundRow row = new OutboundRow();
			row.put("foo_column", Parameter.from(source.foo));
			row.put("entity", Parameter.from("nested_entity"));

			return row;
		}
	}

	@ReadingConverter
	enum RowToCustomConversionPerson implements Converter<Row, CustomConversionPerson> {

		INSTANCE;

		@Override
		public CustomConversionPerson convert(Row source) {

			CustomConversionPerson person = new CustomConversionPerson();
			person.foo = source.get("foo_column", String.class);

			Object nested_entity = source.get("nested_entity");
			person.entity = nested_entity != null ? new NonMappableEntity() : null;

			return person;
		}
	}
}
