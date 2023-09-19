/*
 * Copyright 2017-2023 the original author or authors.
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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * Maps a {@link ResultSet} to an entity of type {@code T}, including entities referenced. This {@link RowMapper} might
 * trigger additional SQL statements in order to load other members of the same aggregate.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Bastian Wilhelm
 * @since 1.1
 */
public class EntityRowMapper<T> implements RowMapper<T> {

	private final RelationalPersistentEntity<T> entity;
	private final AggregatePath path;
	private final JdbcConverter converter;
	private final Identifier identifier;

	/**
	 * @deprecated use {@link EntityRowMapper#EntityRowMapper(AggregatePath, JdbcConverter, Identifier)} instead
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	@SuppressWarnings("unchecked")
	public EntityRowMapper(PersistentPropertyPathExtension path, JdbcConverter converter, Identifier identifier) {

		this.entity = (RelationalPersistentEntity<T>) path.getLeafEntity();
		this.path = path.getAggregatePath();
		this.converter = converter;
		this.identifier = identifier;
	}

	@SuppressWarnings("unchecked")
	public EntityRowMapper(AggregatePath path, JdbcConverter converter, Identifier identifier) {

		this.entity = (RelationalPersistentEntity<T>) path.getLeafEntity();
		this.path = path;
		this.converter = converter;
		this.identifier = identifier;
	}

	public EntityRowMapper(RelationalPersistentEntity<T> entity, JdbcConverter converter) {

		this.entity = entity;
		this.path = null;
		this.converter = converter;
		this.identifier = null;
	}

	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

		RowDocument document = toRowDocument(resultSet);

		// TODO: Remove mapRow methods.
		if (true) {
			return path == null //
					? converter.readAndResolve(entity.getType(), document) //
					: converter.readAndResolve(entity.getType(), document, identifier);
		}

		return path == null //
				? converter.mapRow(entity, resultSet, rowNumber) //
				: converter.mapRow(path, resultSet, identifier, rowNumber);
	}

	/**
	 * Create a {@link RowDocument} from the current {@link ResultSet} row.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @return
	 * @throws SQLException
	 */
	static RowDocument toRowDocument(ResultSet resultSet) throws SQLException {

		ResultSetMetaData md = resultSet.getMetaData();
		int columnCount = md.getColumnCount();
		RowDocument document = new RowDocument(columnCount);

		for (int i = 0; i < columnCount; i++) {
			Object rsv = JdbcUtils.getResultSetValue(resultSet, i + 1);
			String columnName = md.getColumnLabel(i + 1);
			document.put(columnName, rsv instanceof Array a ? a.getArray() : rsv);
		}

		return document;
	}
}
