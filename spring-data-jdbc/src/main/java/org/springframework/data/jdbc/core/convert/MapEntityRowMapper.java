/*
 * Copyright 2017-2024 the original author or authors.
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

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.RowMapper;

/**
 * A {@link RowMapper} that maps a row to a {@link Map.Entry} so an {@link Iterable} of those can be converted to a
 * {@link Map} using an {@link IterableOfEntryToMapConverter}. Creation of the {@literal value} part of the resulting
 * {@link Map.Entry} is delegated to a {@link RowMapper} provided in the constructor.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 */
class MapEntityRowMapper<T> implements RowMapper<Map.Entry<Object, T>> {

	private final AggregatePath path;
	private final JdbcConverter converter;
	private final Identifier identifier;
	private final SqlIdentifier keyColumn;

	MapEntityRowMapper(AggregatePath path, JdbcConverter converter, Identifier identifier, SqlIdentifier keyColumn) {

		this.path = path;
		this.converter = converter;
		this.identifier = identifier;
		this.keyColumn = keyColumn;
	}

	@Override
	public Map.Entry<Object, T> mapRow(ResultSet rs, int rowNum) throws SQLException {

		RowDocument document = RowDocumentResultSetExtractor.toRowDocument(rs);

		Object key = document.get(keyColumn.getReference());
		Class<?> qualifierColumnType = path.getRequiredLeafProperty().getQualifierColumnType();
		Object convertedKey = converter.readValue(key, TypeInformation.of(qualifierColumnType));

		return new HashMap.SimpleEntry<>(convertedKey, mapEntity(document, key));
	}

	@SuppressWarnings("unchecked")
	private T mapEntity(RowDocument document, Object key) {

		return (T) converter.readAndResolve(path.getRequiredLeafEntity().getTypeInformation(), document,
				identifier.withPart(keyColumn, key, Object.class));
	}
}
