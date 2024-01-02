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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.lang.Nullable;

/**
 * Despite its name not really a {@link ResultSet}, but it offers the part of the {@literal ResultSet} API that is used
 * by {@link AggregateReader}. It allows peeking in the next row of a ResultSet by caching one row of the ResultSet.
 *
 * @author Jens Schauder
 * @since 3.2
 */
class CachingResultSet {

	private final ResultSetAccessor accessor;
	private final ResultSet resultSet;
	private Cache cache;

	CachingResultSet(ResultSet resultSet) {

		this.accessor = new ResultSetAccessor(resultSet);
		this.resultSet = resultSet;
	}

	public boolean next() {

		if (isPeeking()) {

			final boolean next = cache.next;
			cache = null;
			return next;
		}

		try {
			return resultSet.next();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to advance CachingResultSet", e);
		}
	}

	@Nullable
	public Object getObject(String columnLabel) {

		Object returnValue;
		if (isPeeking()) {
			returnValue = cache.values.get(columnLabel);
		} else {
			returnValue = safeGetFromDelegate(columnLabel);
		}

		return returnValue;
	}

	@Nullable
	Object peek(String columnLabel) {

		if (!isPeeking()) {
			createCache();
		}

		if (!cache.next) {
			return null;
		}

		return safeGetFromDelegate(columnLabel);
	}

	@Nullable
	private Object safeGetFromDelegate(String columnLabel) {
		return accessor.getObject(columnLabel);
	}

	private void createCache() {
		cache = new Cache();

		try {
			int columnCount = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				// at least some databases return lower case labels although rs.getObject(UPPERCASE_LABEL) returns the expected
				// value. The aliases we use happen to be uppercase. So we transform everything to upper case.
				cache.add(resultSet.getMetaData().getColumnLabel(i).toLowerCase(),
						accessor.getObject(resultSet.getMetaData().getColumnLabel(i)));
			}

			cache.next = resultSet.next();
		} catch (SQLException se) {
			throw new RuntimeException("Can't cache result set data", se);
		}

	}

	private boolean isPeeking() {
		return cache != null;
	}

	private static class Cache {

		boolean next;
		Map<String, Object> values = new HashMap<>();

		void add(String columnName, Object value) {
			values.put(columnName, value);
		}
	}
}
