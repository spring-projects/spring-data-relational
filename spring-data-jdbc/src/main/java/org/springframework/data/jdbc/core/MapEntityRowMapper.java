/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;

/**
 * A {@link RowMapper} that maps a row to a {@link Map.Entry} so an {@link Iterable} of those can be converted to a
 * {@link Map} using an {@link IterableOfEntryToMapConverter}. Creation of the {@literal value} part of the resulting
 * {@link Map.Entry} is delegated to a {@link RowMapper} provided in the constructor.
 *
 * @author Jens Schauder
 */
class MapEntityRowMapper<T> implements RowMapper<Map.Entry<Object, T>> {

	private final RowMapper<T> delegate;
	private final String keyColumn;

	/**
	 * @param delegate rowmapper used as a delegate for obtaining the map values.
	 * @param keyColumn the name of the key column.
	 */
	MapEntityRowMapper(RowMapper<T> delegate, String keyColumn) {

		this.delegate = delegate;
		this.keyColumn = keyColumn;
	}

	@NonNull
	@Override
	public Map.Entry<Object, T> mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new HashMap.SimpleEntry<>(rs.getObject(keyColumn), delegate.mapRow(rs, rowNum));
	}
}
