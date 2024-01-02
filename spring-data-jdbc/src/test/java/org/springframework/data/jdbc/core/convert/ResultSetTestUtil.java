/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.util.Assert;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Utility for mocking ResultSets for tests.
 *
 * @author Jens Schauder
 */
class ResultSetTestUtil {

	static ResultSet mockResultSet(List<String> columns, Object... values) {

		Assert.isTrue( //
				values.length % columns.size() == 0, //
				String //
						.format( //
								"Number of values [%d] must be a multiple of the number of columns [%d]", //
								values.length, //
								columns.size() //
						) //
		);

		List<Map<String, Object>> result = convertValues(columns, values);

		return mock(ResultSet.class, new ResultSetAnswer(columns, result));
	}

	private static List<Map<String, Object>> convertValues(List<String> columns, Object[] values) {

		List<Map<String, Object>> result = new ArrayList<>();

		int index = 0;
		while (index < values.length) {

			Map<String, Object> row = new LinkedCaseInsensitiveMap<>();
			result.add(row);
			for (String column : columns) {

				row.put(column, values[index]);
				index++;
			}
		}
		return result;
	}

	private static class ResultSetAnswer implements Answer<Object> {

		private final List<String> names;
		private final List<Map<String, Object>> values;
		private int index = -1;

		ResultSetAnswer(List<String> names, List<Map<String, Object>> values) {

			this.names = names;
			this.values = values;
		}

		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {

			switch (invocation.getMethod().getName()) {
				case "close" -> {
					close();
					return null;
				}
				case "next" -> {
					return next();
				}
				case "getObject" -> {
					Object argument = invocation.getArgument(0);
					String name = argument instanceof Integer ? names.get(((Integer) argument) - 1) : (String) argument;
					return getObject(name);
				}
				case "isAfterLast" -> {
					return isAfterLast();
				}
				case "isBeforeFirst" -> {
					return isBeforeFirst();
				}
				case "getRow" -> {
					return isAfterLast() || isBeforeFirst() ? 0 : index + 1;
				}
				case "toString" -> {
					return this.toString();
				}
				case "findColumn" -> {
					return findColumn(invocation.getArgument(0));
				}
				case "getMetaData" -> {
					return new MockedMetaData();
				}
				default -> throw new OperationNotSupportedException(invocation.getMethod().getName());
			}
		}

		private int findColumn(String name) {
			if (names.contains(name)) {
				return names.indexOf(name) + 1;
			}

			return -1;
		}

		private boolean isAfterLast() {
			return index >= values.size() && !values.isEmpty();
		}

		private boolean isBeforeFirst() {
			return index < 0 && !values.isEmpty();
		}

		private Object getObject(String column) throws SQLException {

			if (index == -1) {
				throw new SQLException("ResultSet.isBeforeFirst. Make sure to call next() before calling this method");
			}

			Map<String, Object> rowMap = values.get(index);

			if (!rowMap.containsKey(column)) {
				throw new SQLException(String.format("Trying to access a column (%s) that does not exist", column));
			}

			return rowMap.get(column);
		}

		private boolean close() {

			index = -1;
			return index < values.size();
		}

		private boolean next() {

			index++;
			return index < values.size();
		}

		private class MockedMetaData implements ResultSetMetaData {
			@Override
			public int getColumnCount() {
				return names.size();
			}

			@Override
			public boolean isAutoIncrement(int i) {
				return false;
			}

			@Override
			public boolean isCaseSensitive(int i) {
				return false;
			}

			@Override
			public boolean isSearchable(int i) {
				return false;
			}

			@Override
			public boolean isCurrency(int i) {
				return false;
			}

			@Override
			public int isNullable(int i) {
				return 0;
			}

			@Override
			public boolean isSigned(int i) {
				return false;
			}

			@Override
			public int getColumnDisplaySize(int i) {
				return 0;
			}

			@Override
			public String getColumnLabel(int i) {
				return names.get(i - 1);
			}

			@Override
			public String getColumnName(int i) {
				return null;
			}

			@Override
			public String getSchemaName(int i) {
				return null;
			}

			@Override
			public int getPrecision(int i) {
				return 0;
			}

			@Override
			public int getScale(int i) {
				return 0;
			}

			@Override
			public String getTableName(int i) {
				return null;
			}

			@Override
			public String getCatalogName(int i) {
				return null;
			}

			@Override
			public int getColumnType(int i) {
				return 0;
			}

			@Override
			public String getColumnTypeName(int i) {
				return null;
			}

			@Override
			public boolean isReadOnly(int i) {
				return false;
			}

			@Override
			public boolean isWritable(int i) {
				return false;
			}

			@Override
			public boolean isDefinitelyWritable(int i) {
				return false;
			}

			@Override
			public String getColumnClassName(int i) {
				return null;
			}

			@Override
			public <T> T unwrap(Class<T> aClass) {
				return null;
			}

			@Override
			public boolean isWrapperFor(Class<?> aClass) {
				return false;
			}
		}
	}

}
