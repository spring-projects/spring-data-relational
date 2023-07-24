/*
 * Copyright 2019-2023 the original author or authors.
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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToBooleanConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToLocalDateConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToLocalDateTimeConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToLocalTimeConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToNumberConverterFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToOffsetDateTimeConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToStringConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToUuidConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverters.RowToZonedDateTimeConverter;

/**
 * Unit tests for {@link R2dbcConverters}.
 *
 * @author Hebert Coelho
 * @author Mark Paluch
 * @author Valeriy Vyrva
 */
public class R2dbcConvertersUnitTests {

	@Test // gh-41
	public void isReturningAllCreatedConverts() {
		assertThat(R2dbcConverters.getConvertersToRegister()).hasSize(9);
	}

	@Test // gh-41
	public void isConvertingBoolean() {

		Row row = mock(Row.class);
		when(row.get(0, Boolean.class)).thenReturn(true);

		assertThat(RowToBooleanConverter.INSTANCE.convert(row)).isTrue();
	}

	@Test // gh-41
	public void isConvertingLocalDate() {

		LocalDate now = LocalDate.now();
		Row row = mock(Row.class);
		when(row.get(0, LocalDate.class)).thenReturn(now);

		assertThat(RowToLocalDateConverter.INSTANCE.convert(row)).isEqualTo(now);
	}

	@Test // gh-41
	public void isConvertingLocalDateTime() {

		LocalDateTime now = LocalDateTime.now();
		Row row = mock(Row.class);
		when(row.get(0, LocalDateTime.class)).thenReturn(now);

		assertThat(RowToLocalDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
	}

	@Test // gh-41
	public void isConvertingLocalTime() {

		LocalTime now = LocalTime.now();
		Row row = mock(Row.class);
		when(row.get(0, LocalTime.class)).thenReturn(now);

		assertThat(RowToLocalTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
	}

	@Test // gh-41
	public void isConvertingOffsetDateTime() {

		OffsetDateTime now = OffsetDateTime.now();
		Row row = mock(Row.class);
		when(row.get(0, OffsetDateTime.class)).thenReturn(now);

		assertThat(RowToOffsetDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
	}

	@Test // gh-41
	public void isConvertingString() {

		String value = "aValue";
		Row row = mock(Row.class);
		when(row.get(0, String.class)).thenReturn(value);

		assertThat(RowToStringConverter.INSTANCE.convert(row)).isEqualTo(value);
	}

	@Test // gh-41
	public void isConvertingUUID() {

		UUID value = UUID.randomUUID();
		Row row = mock(Row.class);
		when(row.get(0, UUID.class)).thenReturn(value);

		assertThat(RowToUuidConverter.INSTANCE.convert(row)).isEqualTo(value);
	}

	@Test // gh-41
	public void isConvertingZonedDateTime() {

		ZonedDateTime now = ZonedDateTime.now();
		Row row = mock(Row.class);
		when(row.get(0, ZonedDateTime.class)).thenReturn(now);

		assertThat(RowToZonedDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
	}

	@Test // gh-41
	public void isConvertingNumber() {

		Row row = mock(Row.class);
		when(row.get(0)).thenReturn(33);

		final Converter<Row, Integer> converter = RowToNumberConverterFactory.INSTANCE.getConverter(Integer.class);

		assertThat(converter.convert(row)).isEqualTo(33);
	}

	@Test // gh-41
	public void isRaisingExceptionForInvalidNumber() {
		assertThatIllegalArgumentException().isThrownBy(() -> RowToNumberConverterFactory.INSTANCE.getConverter(null));
	}
}
