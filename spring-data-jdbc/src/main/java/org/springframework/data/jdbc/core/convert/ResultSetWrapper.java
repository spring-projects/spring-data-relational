/*
 * Copyright 2020 the original author or authors.
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.MappingException;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Wraps a {@link java.sql.ResultSet} in order to provide fast lookup of columns by name, including for missing columns.
 *
 * @author Jens Schauder
 */
class ResultSetWrapper {

	private static final Logger LOG = LoggerFactory.getLogger(ResultSetWrapper.class);

	private final ResultSet resultSet;

	private final Map<String, Integer> indexLookUp;

	ResultSetWrapper(ResultSet resultSet) {

		this.resultSet = resultSet;
		indexLookUp = indexColumns();
	}

	private Map<String, Integer> indexColumns() {

		try {

			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();

			Map<String, Integer> index = new LinkedCaseInsensitiveMap<>(columnCount);

			for (int i = 1; i <= columnCount; i++) {

				String label = metaData.getColumnLabel(i);
				if (index.containsKey(label)) {
					LOG.warn("We encountered a ResutSet with multiple columns associated with the same label {}.", label);
					continue;
				}
				index.put(label, i);

			}

			return index;
		} catch (SQLException se) {
			throw new MappingException("Failure while accessing metadata.");
		}

	}

	@Nullable
	Object getObject(String columnName) {

		try {

			int column = findColumnIndex(columnName);

			return column > 0 ? resultSet.getObject(column) : SpecialColumnValue.NO_SUCH_COLUMN;
		} catch (SQLException o_O) {
			throw new MappingException(String.format("Could not read value %s from result set!", columnName), o_O);
		}
	}

	private int findColumnIndex(String columnName) {
		return indexLookUp.getOrDefault(columnName, -1);
	}

	enum SpecialColumnValue {
		NO_SUCH_COLUMN
	}
}
