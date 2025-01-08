/*
 * Copyright 2017-2025 the original author or authors.
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

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.RowMapper;

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

	private final TypeInformation<T> typeInformation;
	private final JdbcConverter converter;
	private final Identifier identifier;

	private EntityRowMapper(TypeInformation<T> typeInformation, JdbcConverter converter, Identifier identifier) {

		this.typeInformation = typeInformation;
		this.converter = converter;
		this.identifier = identifier;
	}

	@SuppressWarnings("unchecked")
	public EntityRowMapper(AggregatePath path, JdbcConverter converter, Identifier identifier) {
		this(((RelationalPersistentEntity<T>) path.getRequiredLeafEntity()).getTypeInformation(), converter, identifier);
	}

	public EntityRowMapper(RelationalPersistentEntity<T> entity, JdbcConverter converter) {
		this(entity.getTypeInformation(), converter, Identifier.empty());
	}

	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

		RowDocument document = RowDocumentResultSetExtractor.toRowDocument(resultSet);

		return converter.readAndResolve(typeInformation, document, identifier);
	}

}
