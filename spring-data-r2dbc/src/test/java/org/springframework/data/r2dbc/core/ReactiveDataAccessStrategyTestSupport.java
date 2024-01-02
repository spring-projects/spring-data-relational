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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockRowMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * Abstract base class for {@link R2dbcDialect}-aware {@link DefaultReactiveDataAccessStrategy} tests.
 *
 * @author Mark Paluch
 * @author Louis Morgan
 */
public abstract class ReactiveDataAccessStrategyTestSupport {

	protected abstract ReactiveDataAccessStrategy getStrategy();

	@Test // gh-85
	void shouldReadAndWriteString() {
		testType((pt, s) -> pt.string = s, pt -> pt.string, "foo", "string");
	}

	@Test // gh-85
	void shouldReadAndWriteCharacter() {
		testType((pt, c) -> pt.character = c, pt -> pt.character, 'f', "character");
	}

	@Test // gh-85
	void shouldReadAndWriteBoolean() {
		testType((pt, b) -> pt.booleanValue = b, pt -> pt.booleanValue, true, "boolean_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedBoolean() {
		testType((pt, b) -> pt.boxedBooleanValue = b, pt -> pt.boxedBooleanValue, true, "boxed_boolean_value");
	}

	@Test // gh-85
	void shouldReadAndWriteByte() {
		testType((pt, b) -> pt.byteValue = b, pt -> pt.byteValue, (byte) 123, "byte_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedByte() {
		testType((pt, b) -> pt.boxedByteValue = b, pt -> pt.boxedByteValue, (byte) 123, "boxed_byte_value");
	}

	@Test // gh-85
	void shouldReadAndWriteShort() {
		testType((pt, s) -> pt.shortValue = s, pt -> pt.shortValue, (short) 123, "short_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedShort() {
		testType((pt, b) -> pt.boxedShortValue = b, pt -> pt.boxedShortValue, (short) 123, "boxed_short_value");
	}

	@Test // gh-85
	void shouldReadAndWriteInteger() {
		testType((pt, i) -> pt.intValue = i, pt -> pt.intValue, 123, "int_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedInteger() {
		testType((pt, b) -> pt.boxedIntegerValue = b, pt -> pt.boxedIntegerValue, 123, "boxed_integer_value");
	}

	@Test // gh-85
	void shouldReadAndWriteLong() {
		testType((pt, l) -> pt.longValue = l, pt -> pt.longValue, 123L, "long_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedLong() {
		testType((pt, b) -> pt.boxedLongValue = b, pt -> pt.boxedLongValue, 123L, "boxed_long_value");
	}

	@Test // gh-85
	void shouldReadAndWriteFloat() {
		testType((pt, f) -> pt.floatValue = f, pt -> pt.floatValue, 0.1f, "float_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedFloat() {
		testType((pt, b) -> pt.boxedFloatValue = b, pt -> pt.boxedFloatValue, 0.1f, "boxed_float_value");
	}

	@Test // gh-85
	void shouldReadAndWriteDouble() {
		testType((pt, d) -> pt.doubleValue = d, pt -> pt.doubleValue, 0.1, "double_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBoxedDouble() {
		testType((pt, b) -> pt.boxedDoubleValue = b, pt -> pt.boxedDoubleValue, 0.1, "boxed_double_value");
	}

	@Test // gh-85
	void shouldReadAndWriteBigInteger() {
		testType((pt, b) -> pt.bigInteger = b, pt -> pt.bigInteger, BigInteger.TEN, "big_integer");
	}

	@Test // gh-85
	void shouldReadAndWriteBigDecimal() {
		testType((pt, b) -> pt.bigDecimal = b, pt -> pt.bigDecimal, new BigDecimal("100.123"), "big_decimal");
	}

	@Test // gh-85
	void shouldReadAndWriteLocalDate() {
		testType((pt, l) -> pt.localDate = l, pt -> pt.localDate, LocalDate.now(), "local_date");
	}

	@Test // gh-85
	void shouldReadAndWriteLocalTime() {
		testType((pt, l) -> pt.localTime = l, pt -> pt.localTime, LocalTime.now(), "local_time");
	}

	@Test // gh-85
	void shouldReadAndWriteLocalDateTime() {
		testType((pt, l) -> pt.localDateTime = l, pt -> pt.localDateTime, LocalDateTime.now(), "local_date_time");
	}

	@Test // gh-85
	void shouldReadAndWriteZonedDateTime() {
		testType((pt, z) -> pt.zonedDateTime = z, pt -> pt.zonedDateTime, ZonedDateTime.now(), "zoned_date_time");
	}

	@Test // gh-85
	void shouldReadAndWriteOffsetDateTime() {
		testType((pt, o) -> pt.offsetDateTime = o, pt -> pt.offsetDateTime, OffsetDateTime.now(), "offset_date_time");
	}

	@Test // gh-85
	void shouldReadAndWriteUuid() {
		testType((pt, u) -> pt.uuid = u, pt -> pt.uuid, UUID.randomUUID(), "uuid");
	}

	@Test // gh-186
	void shouldReadAndWriteBinary() {
		testType((pt, b) -> pt.binary = b, pt -> pt.binary, "hello".getBytes(), "binary");
	}

	@Test // gh-354
	void shouldNotWriteReadOnlyFields() {

		TypeWithReadOnlyFields toSave = new TypeWithReadOnlyFields();

		toSave.writableField = "writable";
		toSave.readOnlyField = "readonly";
		toSave.readOnlyArrayField = "readonly_array".getBytes();

		assertThat(getStrategy().getOutboundRow(toSave)).containsOnlyKeys(SqlIdentifier.unquoted("writable_field"));
	}

	private <T> void testType(BiConsumer<PrimitiveTypes, T> setter, Function<PrimitiveTypes, T> getter, T testValue,
			String fieldname) {

		ReactiveDataAccessStrategy strategy = getStrategy();
		Row rowMock = mock(Row.class);
		RowMetadata metadataMock = MockRowMetadata.builder()
				.columnMetadata(
						MockColumnMetadata.builder().name(fieldname).type(Parameters.in(testValue.getClass()).getType()).build())
				.build();

		PrimitiveTypes toSave = new PrimitiveTypes();
		setter.accept(toSave, testValue);

		assertThat(strategy.getOutboundRow(toSave)).containsEntry(SqlIdentifier.unquoted(fieldname),
				Parameter.from(testValue));

		when(rowMock.get(fieldname)).thenReturn(testValue);

		PrimitiveTypes loaded = strategy.getRowMapper(PrimitiveTypes.class).apply(rowMock, metadataMock);

		assertThat(getter.apply(loaded)).isEqualTo(testValue);
	}

	static class PrimitiveTypes {

		String string;
		char character;

		boolean booleanValue;
		byte byteValue;
		short shortValue;
		int intValue;
		long longValue;
		double doubleValue;
		float floatValue;

		Boolean boxedBooleanValue;
		Byte boxedByteValue;
		Short boxedShortValue;
		Integer boxedIntegerValue;
		Long boxedLongValue;
		Double boxedDoubleValue;
		Float boxedFloatValue;

		BigInteger bigInteger;
		BigDecimal bigDecimal;

		LocalDate localDate;
		LocalTime localTime;
		LocalDateTime localDateTime;
		OffsetDateTime offsetDateTime;
		ZonedDateTime zonedDateTime;

		byte[] binary;

		UUID uuid;
	}

	static class TypeWithReadOnlyFields {
		String writableField;
		@ReadOnlyProperty String readOnlyField;
		@ReadOnlyProperty byte[] readOnlyArrayField;
	}
}
