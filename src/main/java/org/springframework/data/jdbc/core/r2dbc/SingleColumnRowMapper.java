/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.r2dbc;

import java.util.Collection;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.TypeMismatchDataAccessException;
import org.springframework.jdbc.IncorrectResultSetColumnCountException;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

/**
 * {@link RowMapper} implementation that converts a single column into a single result value per row. Expects to operate
 * on a {@link Row} that just contains a single column.
 * <p>
 * The type of the result value for each row can be specified. The value for the single column will be extracted from
 * the {@link io.r2dbc.spi.Result} and converted into the specified target type.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate#queryForFlux(String, Class)
 * @see R2dbcTemplate#queryForObject(String, Class)
 */
public class SingleColumnRowMapper<T> implements RowMapper<T> {

	@Nullable private Class<?> requiredType;

	@Nullable private ConversionService conversionService = DefaultConversionService.getSharedInstance();

	/**
	 * Create a new {@code SingleColumnRowMapper} for bean-style configuration.
	 *
	 * @see #setRequiredType
	 */
	public SingleColumnRowMapper() {}

	/**
	 * Create a new {@code SingleColumnRowMapper}.
	 * <p>
	 * Consider using the {@link #newInstance} factory method instead, which allows for specifying the required type once
	 * only.
	 *
	 * @param requiredType the type that each result object is expected to match
	 */
	public SingleColumnRowMapper(Class<T> requiredType) {
		setRequiredType(requiredType);
	}

	/**
	 * Set the type that each result object is expected to match.
	 * <p>
	 * If not specified, the column value will be exposed as returned by the R2DBC driver.
	 */
	public void setRequiredType(Class<T> requiredType) {
		this.requiredType = ClassUtils.resolvePrimitiveIfNecessary(requiredType);
	}

	/**
	 * Set a {@link ConversionService} for converting a fetched value.
	 * <p>
	 * Default is the {@link DefaultConversionService}.
	 *
	 * @see DefaultConversionService#getSharedInstance
	 */
	public void setConversionService(@Nullable ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	@Override
	public T mapRow(Row rs) {
		throw new UnsupportedOperationException("Should never be called by the framework!");
	}

	/**
	 * Extract a value for the single column in the current row.
	 * <p>
	 * Validates that there is only one column selected, then delegates to {@code getColumnValue()} and also
	 * {@code convertValueToRequiredType}, if necessary.
	 *
	 * @see RowMetadata#getColumnMetadatas()
	 * @see #getColumnValue(Row, int, Class)
	 * @see #convertValueToRequiredType(Object, Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T apply(Row row, RowMetadata rowMetadata) {

		// Validate column count.
		Collection<? extends ColumnMetadata> columns = IterableUtils.toCollection(rowMetadata.getColumnMetadatas());
		int nrOfColumns = columns.size();
		if (nrOfColumns != 1) {
			throw new IncorrectResultSetColumnCountException(1, nrOfColumns);
		}

		// Extract column value from R2DBC ResultSet.
		Object result = getColumnValue(row, 1, this.requiredType);
		if (result != null && this.requiredType != null && !this.requiredType.isInstance(result)) {
			// Extracted value does not match already: try to convert it.
			try {
				return (T) convertValueToRequiredType(result, this.requiredType);
			} catch (IllegalArgumentException ex) {
				throw new TypeMismatchDataAccessException("Type mismatch affecting row number TODO and column type '"
						+ columns.iterator().next().getName() + "': " + ex.getMessage());
			}
		}
		return (T) result;
	}

	/**
	 * Retrieve a R2DBC object value for the specified column.
	 *
	 * @param row is the ResultSet holding the data
	 * @param index is the column index
	 * @param requiredType the type that each result object is expected to match (or {@code null} if none specified)
	 * @return the Object value
	 * @see #getColumnValue(Row, int)
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index, @Nullable Class<?> requiredType) {
		if (requiredType != null) {
			return row.get(index, requiredType);
		} else {
			// No required type specified -> perform default extraction.
			return getColumnValue(row, index);
		}
	}

	/**
	 * Retrieve a R2DBC object value for the specified column, using the most appropriate value type. Called if no
	 * required type has been specified.
	 * <p>
	 * The default implementation delegates to {@link Row#get(Object, Class)}.
	 *
	 * @param row is the ResultSet holding the data
	 * @param index is the column index
	 * @return the Object value
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index) {
		return row.get(index, Object.class);
	}

	/**
	 * Convert the given column value to the specified required type. Only called if the extracted column value does not
	 * match already.
	 * <p>
	 * If the required type is String, the value will simply get stringified via {@code toString()}. In case of a Number,
	 * the value will be converted into a Number, either through number conversion or through String parsing (depending on
	 * the value type). Otherwise, the value will be converted to a required type using the {@link ConversionService}.
	 *
	 * @param value the column value as extracted from {@code getColumnValue()} (never {@code null})
	 * @param requiredType the type that each result object is expected to match (never {@code null})
	 * @return the converted value
	 * @see #getColumnValue(Row, int, Class)
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	protected Object convertValueToRequiredType(Object value, Class<?> requiredType) {

		if (String.class == requiredType) {
			return value.toString();
		} else if (Number.class.isAssignableFrom(requiredType)) {
			if (value instanceof Number) {
				// Convert original Number to target Number class.
				return NumberUtils.convertNumberToTargetClass(((Number) value), (Class<Number>) requiredType);
			} else {
				// Convert stringified value to target Number class.
				return NumberUtils.parseNumber(value.toString(), (Class<Number>) requiredType);
			}
		} else if (this.conversionService != null && this.conversionService.canConvert(value.getClass(), requiredType)) {
			return this.conversionService.convert(value, requiredType);
		} else {
			throw new IllegalArgumentException("Value [" + value + "] is of type [" + value.getClass().getName()
					+ "] and cannot be converted to required type [" + requiredType.getName() + "]");
		}
	}

	/**
	 * Static factory method to create a new {@code SingleColumnRowMapper} (with the required type specified only once).
	 *
	 * @param requiredType the type that each result object is expected to match
	 */
	public static <T> SingleColumnRowMapper<T> newInstance(Class<T> requiredType) {
		return new SingleColumnRowMapper<>(requiredType);
	}

	/**
	 * Static factory method to create a new {@code SingleColumnRowMapper} (with the required type specified only once).
	 *
	 * @param requiredType the type that each result object is expected to match
	 * @param conversionService the {@link ConversionService} for converting a fetched value
	 */
	public static <T> SingleColumnRowMapper<T> newInstance(Class<T> requiredType,
			@Nullable ConversionService conversionService) {
		SingleColumnRowMapper<T> rowMapper = newInstance(requiredType);
		rowMapper.setConversionService(conversionService);
		return rowMapper;
	}
}
