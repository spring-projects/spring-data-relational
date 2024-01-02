/*
 * Copyright 2019-2024 the original author or authors.
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

import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalConverter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * Postgres-specific unit tests for {@link MappingR2dbcConverter}.
 *
 * @author Mark Paluch
 */
class PostgresMappingR2dbcConverterUnitTests {

	private RelationalMappingContext mappingContext = new R2dbcMappingContext();
	private MappingR2dbcConverter converter = new MappingR2dbcConverter(mappingContext);

	@BeforeEach
	void before() {

		List<Object> converters = new ArrayList<>(PostgresDialect.INSTANCE.getConverters());
		converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);
		CustomConversions.StoreConversions storeConversions = CustomConversions.StoreConversions
				.of(PostgresDialect.INSTANCE.getSimpleTypeHolder(), converters);

		R2dbcCustomConversions customConversions = new R2dbcCustomConversions(storeConversions,
				Arrays.asList(JsonToJsonHolderConverter.INSTANCE, JsonHolderToJsonConverter.INSTANCE));

		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		converter = new MappingR2dbcConverter(mappingContext, customConversions);
	}

	@Test // gh-318
	void shouldPassThruJson() {

		JsonPerson person = new JsonPerson(null, Json.of("{\"hello\":\"world\"}"));

		OutboundRow row = new OutboundRow();
		converter.write(person, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("json_value"), Parameter.from(person.jsonValue));
	}

	@Test // gh-453
	void shouldConvertJsonToString() {

		MockRow row = MockRow.builder().identified("json_string", Object.class, Json.of("{\"hello\":\"world\"}")).build();

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("json_string").type(R2dbcType.VARCHAR).build()).build();

		ConvertedJson result = converter.read(ConvertedJson.class, row, metadata);
		assertThat(result.jsonString).isEqualTo("{\"hello\":\"world\"}");
	}

	@Test // gh-453
	void shouldConvertJsonToByteArray() {

		MockRow row = MockRow.builder().identified("json_bytes", Object.class, Json.of("{\"hello\":\"world\"}")).build();

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("json_bytes").type(R2dbcType.VARCHAR).build()).build();

		ConvertedJson result = converter.read(ConvertedJson.class, row, metadata);
		assertThat(result.jsonBytes).isEqualTo("{\"hello\":\"world\"}".getBytes());
	}

	@Test // gh-585
	void shouldApplyCustomReadingConverter() {

		MockRow row = MockRow.builder().identified("holder", Object.class, Json.of("{\"hello\":\"world\"}")).build();

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("holder").type(R2dbcType.VARCHAR).build()).build();

		WithJsonHolder result = converter.read(WithJsonHolder.class, row, metadata);
		assertThat(result.holder).isNotNull();
		assertThat(result.holder.json).isNotNull();
	}

	@Test // gh-585
	void shouldApplyCustomWritingConverter() {

		WithJsonHolder object = new WithJsonHolder(new JsonHolder(Json.of("{\"hello\":\"world\"}")));

		OutboundRow row = new OutboundRow();
		converter.write(object, row);

		Parameter parameter = row.get(SqlIdentifier.unquoted("holder"));
		assertThat(parameter).isNotNull();
		assertThat(parameter.getValue()).isInstanceOf(Json.class);
	}

	record JsonPerson(
			@Id Long id,
			Json jsonValue) {
	}

	record ConvertedJson(
			@Id Long id,
			String jsonString,
			byte[] jsonBytes) {
	}

	record WithJsonHolder(
			JsonHolder holder) {
	}

	@ReadingConverter
	enum JsonToJsonHolderConverter implements GenericConverter, ConditionalConverter {

		INSTANCE;

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			return Json.class.isAssignableFrom(sourceType.getType());
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return Collections.singleton(new GenericConverter.ConvertiblePair(Json.class, Object.class));
		}

		@Override
		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return new JsonHolder((Json) source);
		}
	}

	@WritingConverter
	enum JsonHolderToJsonConverter implements GenericConverter, ConditionalConverter {

		INSTANCE;

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			return JsonHolder.class.isAssignableFrom(sourceType.getType());
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return Collections.singleton(new GenericConverter.ConvertiblePair(JsonHolder.class, Json.class));
		}

		@Override
		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return ((JsonHolder) source).json;
		}
	}

	record JsonHolder(Json json) {

	}

}
