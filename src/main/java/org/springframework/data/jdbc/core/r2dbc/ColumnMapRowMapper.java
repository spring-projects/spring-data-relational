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
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

/**
 * {@link RowMapper} implementation that creates a {@link java.util.Map} for each row, representing all columns as
 * key-value pairs: one entry for each column, with the column name as key.
 * <p>
 * The {@link Map} implementation to use and the key to use for each column in the column Map can be customized through
 * overriding {@link #createColumnMap} and {@link #getColumnKey}, respectively.
 * <p>
 * <b>Note:</b> By default, {@link ColumnMapRowMapper} will try to build a linked {@link Map} with case-insensitive
 * keys, to preserve column order as well as allow any casing to be used for column names. This requires Commons
 * Collections on the classpath (which will be autodetected). Else, the fallback is a standard linked
 * {@link java.util.HashMap}, which will still preserve column order but requires the application to specify the column
 * names in the same casing as exposed by the driver.
 *
 * @author Mark Paluch
 * @see R2dbcTemplate#queryForFlux(String)
 * @see R2dbcTemplate#queryForMap(String)
 */
public class ColumnMapRowMapper implements RowMapper<Map<String, Object>> {

	@Override
	public Map<String, Object> mapRow(Row rs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> apply(Row row, RowMetadata rowMetadata) {

		Collection<? extends ColumnMetadata> columns = IterableUtils.toCollection(rowMetadata.getColumnMetadatas());
		int columnCount = columns.size();
		Map<String, Object> mapOfColValues = createColumnMap(columnCount);

		int index = 0;
		for (ColumnMetadata column : columns) {

			String key = getColumnKey(column.getName());
			Object obj = getColumnValue(row, index++);
			mapOfColValues.put(key, obj);
		}
		return mapOfColValues;
	}

	/**
	 * Create a {@link Map} instance to be used as column map.
	 * <p>
	 * By default, a linked case-insensitive Map will be created.
	 *
	 * @param columnCount the column count, to be used as initial capacity for the Map.
	 * @return the new {@link Map} instance.
	 * @see org.springframework.util.LinkedCaseInsensitiveMap
	 */
	protected Map<String, Object> createColumnMap(int columnCount) {
		return new LinkedCaseInsensitiveMap<>(columnCount);
	}

	/**
	 * Determine the key to use for the given column in the column {@link Map}.
	 *
	 * @param columnName the column name as returned by the {@link Row}.
	 * @return the column key to use.
	 * @see ColumnMetadata#getName()
	 */
	protected String getColumnKey(String columnName) {
		return columnName;
	}

	/**
	 * Retrieve a R2DBC object value for the specified column.
	 * <p>
	 * The default implementation uses the {@link Row#get(Object, Class)} method.
	 *
	 * @param row is the {@link Row} holding the data.
	 * @param index is the column index.
	 * @return the Object returned.
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index) {
		return row.get(index, Object.class);
	}
}
