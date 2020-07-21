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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Test;

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
	public void shouldReadAndWriteString() {
		testType(PrimitiveTypes::setString, PrimitiveTypes::getString, "foo", "string");
	}

	@Test // gh-85
	public void shouldReadAndWriteCharacter() {
		testType(PrimitiveTypes::setCharacter, PrimitiveTypes::getCharacter, 'f', "character");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoolean() {
		testType(PrimitiveTypes::setBooleanValue, PrimitiveTypes::isBooleanValue, true, "boolean_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedBoolean() {
		testType(PrimitiveTypes::setBoxedBooleanValue, PrimitiveTypes::getBoxedBooleanValue, true, "boxed_boolean_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteByte() {
		testType(PrimitiveTypes::setByteValue, PrimitiveTypes::getByteValue, (byte) 123, "byte_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedByte() {
		testType(PrimitiveTypes::setBoxedByteValue, PrimitiveTypes::getBoxedByteValue, (byte) 123, "boxed_byte_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteShort() {
		testType(PrimitiveTypes::setShortValue, PrimitiveTypes::getShortValue, (short) 123, "short_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedShort() {
		testType(PrimitiveTypes::setBoxedShortValue, PrimitiveTypes::getBoxedShortValue, (short) 123, "boxed_short_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteInteger() {
		testType(PrimitiveTypes::setIntValue, PrimitiveTypes::getIntValue, 123, "int_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedInteger() {
		testType(PrimitiveTypes::setBoxedIntegerValue, PrimitiveTypes::getBoxedIntegerValue, 123, "boxed_integer_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteLong() {
		testType(PrimitiveTypes::setLongValue, PrimitiveTypes::getLongValue, 123L, "long_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedLong() {
		testType(PrimitiveTypes::setBoxedLongValue, PrimitiveTypes::getBoxedLongValue, 123L, "boxed_long_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteFloat() {
		testType(PrimitiveTypes::setFloatValue, PrimitiveTypes::getFloatValue, 0.1f, "float_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedFloat() {
		testType(PrimitiveTypes::setBoxedFloatValue, PrimitiveTypes::getBoxedFloatValue, 0.1f, "boxed_float_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteDouble() {
		testType(PrimitiveTypes::setDoubleValue, PrimitiveTypes::getDoubleValue, 0.1, "double_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBoxedDouble() {
		testType(PrimitiveTypes::setBoxedDoubleValue, PrimitiveTypes::getBoxedDoubleValue, 0.1, "boxed_double_value");
	}

	@Test // gh-85
	public void shouldReadAndWriteBigInteger() {
		testType(PrimitiveTypes::setBigInteger, PrimitiveTypes::getBigInteger, BigInteger.TEN, "big_integer");
	}

	@Test // gh-85
	public void shouldReadAndWriteBigDecimal() {
		testType(PrimitiveTypes::setBigDecimal, PrimitiveTypes::getBigDecimal, new BigDecimal("100.123"), "big_decimal");
	}

	@Test // gh-85
	public void shouldReadAndWriteLocalDate() {
		testType(PrimitiveTypes::setLocalDate, PrimitiveTypes::getLocalDate, LocalDate.now(), "local_date");
	}

	@Test // gh-85
	public void shouldReadAndWriteLocalTime() {
		testType(PrimitiveTypes::setLocalTime, PrimitiveTypes::getLocalTime, LocalTime.now(), "local_time");
	}

	@Test // gh-85
	public void shouldReadAndWriteLocalDateTime() {
		testType(PrimitiveTypes::setLocalDateTime, PrimitiveTypes::getLocalDateTime, LocalDateTime.now(),
				"local_date_time");
	}

	@Test // gh-85
	public void shouldReadAndWriteZonedDateTime() {
		testType(PrimitiveTypes::setZonedDateTime, PrimitiveTypes::getZonedDateTime, ZonedDateTime.now(),
				"zoned_date_time");
	}

	@Test // gh-85
	public void shouldReadAndWriteOffsetDateTime() {
		testType(PrimitiveTypes::setOffsetDateTime, PrimitiveTypes::getOffsetDateTime, OffsetDateTime.now(),
				"offset_date_time");
	}

	@Test // gh-85
	public void shouldReadAndWriteUuid() {
		testType(PrimitiveTypes::setUuid, PrimitiveTypes::getUuid, UUID.randomUUID(), "uuid");
	}

	@Test // gh-186
	public void shouldReadAndWriteBinary() {
		testType(PrimitiveTypes::setBinary, PrimitiveTypes::getBinary, "hello".getBytes(), "binary");
	}

	@Test // gh-354
	public void shouldNotWriteReadOnlyFields() {

		TypeWithReadOnlyFields toSave = new TypeWithReadOnlyFields();

		toSave.setWritableField("writable");
		toSave.setReadOnlyField("readonly");
		toSave.setReadOnlyArrayField("readonly_array".getBytes());

		assertThat(getStrategy().getOutboundRow(toSave)).containsOnlyKeys(SqlIdentifier.unquoted("writable_field"));
	}

	private <T> void testType(BiConsumer<PrimitiveTypes, T> setter, Function<PrimitiveTypes, T> getter, T testValue,
			String fieldname) {

		ReactiveDataAccessStrategy strategy = getStrategy();
		Row rowMock = mock(Row.class);
		RowMetadata metadataMock = mock(RowMetadata.class);
		Collection<String> columnNames = mock(Collection.class);
		when(metadataMock.getColumnNames()).thenReturn(columnNames);
		when(columnNames.contains(fieldname)).thenReturn(true);

		PrimitiveTypes toSave = new PrimitiveTypes();
		setter.accept(toSave, testValue);

		assertThat(strategy.getOutboundRow(toSave)).containsEntry(SqlIdentifier.unquoted(fieldname),
				Parameter.from(testValue));

		when(rowMock.get(fieldname)).thenReturn(testValue);

		PrimitiveTypes loaded = strategy.getRowMapper(PrimitiveTypes.class).apply(rowMock, metadataMock);

		assertThat(getter.apply(loaded)).isEqualTo(testValue);
	}

	@Data
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

	@Data
	static class TypeWithReadOnlyFields {
		String writableField;
		@ReadOnlyProperty String readOnlyField;
		@ReadOnlyProperty byte[] readOnlyArrayField;
	}
}
