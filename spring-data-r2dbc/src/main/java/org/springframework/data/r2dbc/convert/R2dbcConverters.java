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

import io.r2dbc.spi.Row;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

/**
 * Wrapper class to contain useful converters for the usage with R2DBC.
 *
 * @author Hebert Coelho
 * @author Mark Paluch
 * @author Valeriy Vyrva
 */
abstract class R2dbcConverters {

	private R2dbcConverters() {}

	/**
	 * @return A list of the registered converters
	 */
	public static Collection<Object> getConvertersToRegister() {

		List<Object> converters = new ArrayList<>();

		converters.add(RowToBooleanConverter.INSTANCE);
		converters.add(RowToNumberConverterFactory.INSTANCE);
		converters.add(RowToLocalDateConverter.INSTANCE);
		converters.add(RowToLocalDateTimeConverter.INSTANCE);
		converters.add(RowToLocalTimeConverter.INSTANCE);
		converters.add(RowToOffsetDateTimeConverter.INSTANCE);
		converters.add(RowToStringConverter.INSTANCE);
		converters.add(RowToUuidConverter.INSTANCE);
		converters.add(RowToZonedDateTimeConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link Boolean} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToBooleanConverter implements Converter<Row, Boolean> {

		INSTANCE;

		@Override
		public Boolean convert(Row row) {
			return row.get(0, Boolean.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link LocalDate} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToLocalDateConverter implements Converter<Row, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(Row row) {
			return row.get(0, LocalDate.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link LocalDateTime} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToLocalDateTimeConverter implements Converter<Row, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(Row row) {
			return row.get(0, LocalDateTime.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link LocalTime} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToLocalTimeConverter implements Converter<Row, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Row row) {
			return row.get(0, LocalTime.class);
		}
	}

	/**
	 * Singleton converter factory to convert the first column of a {@link Row} to a {@link Number}.
	 * <p>
	 * Support Number classes including Byte, Short, Integer, Float, Double, Long, BigInteger, BigDecimal. This class
	 * delegates to {@link NumberUtils#convertNumberToTargetClass(Number, Class)} to perform the conversion.
	 *
	 * @see Byte
	 * @see Short
	 * @see Integer
	 * @see Long
	 * @see java.math.BigInteger
	 * @see Float
	 * @see Double
	 * @see java.math.BigDecimal
	 * @author Hebert Coelho
	 */
	public enum RowToNumberConverterFactory implements ConverterFactory<Row, Number> {

		INSTANCE;

		@Override
		public <T extends Number> Converter<Row, T> getConverter(Class<T> targetType) {
			Assert.notNull(targetType, "Target type must not be null");
			return new RowToNumber<>(targetType);
		}

		static class RowToNumber<T extends Number> implements Converter<Row, T> {

			private final Class<T> targetType;

			RowToNumber(Class<T> targetType) {
				this.targetType = targetType;
			}

			@Override
			public T convert(Row source) {

				Object object = source.get(0);

				return (object != null ? NumberUtils.convertNumberToTargetClass((Number) object, this.targetType) : null);
			}
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link OffsetDateTime} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToOffsetDateTimeConverter implements Converter<Row, OffsetDateTime> {

		INSTANCE;

		@Override
		public OffsetDateTime convert(Row row) {
			return row.get(0, OffsetDateTime.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link String} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToStringConverter implements Converter<Row, String> {

		INSTANCE;

		@Override
		public String convert(Row row) {
			return row.get(0, String.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link UUID} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToUuidConverter implements Converter<Row, UUID> {

		INSTANCE;

		@Override
		public UUID convert(Row row) {
			return row.get(0, UUID.class);
		}
	}

	/**
	 * Simple singleton to convert {@link Row}s to their {@link ZonedDateTime} representation.
	 *
	 * @author Hebert Coelho
	 */
	public enum RowToZonedDateTimeConverter implements Converter<Row, ZonedDateTime> {

		INSTANCE;

		@Override
		public ZonedDateTime convert(Row row) {
			return row.get(0, ZonedDateTime.class);
		}
	}

}
