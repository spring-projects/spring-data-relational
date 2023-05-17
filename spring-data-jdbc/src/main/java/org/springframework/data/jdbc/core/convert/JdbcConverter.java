	/*
 * Copyright 2019-2023 the original author or authors.
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
import java.sql.SQLType;

import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * A {@link JdbcConverter} is responsible for converting for values to the native relational representation and vice
 * versa.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public interface JdbcConverter extends RelationalConverter {

	/**
	 * Convert a property value into a {@link JdbcValue} that contains the converted value and information how to bind it
	 * to JDBC parameters.
	 *
	 * @param value a value as it is used in the object model. May be {@code null}.
	 * @param type {@literal Class} into which the value is to be converted. Must not be {@code null}.
	 * @param sqlType the {@link SQLType} to be used if non is specified by a converter.
	 * @return The converted value wrapped in a {@link JdbcValue}. Guaranteed to be not {@literal null}.
	 * @since 2.4
	 */
	JdbcValue writeJdbcValue(@Nullable Object value, Class<?> type, SQLType sqlType);

	/**
	 * Read the current row from {@link ResultSet} to an {@link RelationalPersistentEntity#getType() entity}.
	 *
	 * @param entity the persistent entity type.
	 * @param resultSet the {@link ResultSet} to read from.
	 * @param key primary key.
	 * @param <T>
	 * @return
	 */
	<T> T mapRow(RelationalPersistentEntity<T> entity, ResultSet resultSet, Object key);

	/**
	 * Read the current row from {@link ResultSet} to an {@link PersistentPropertyPathExtension#getActualType() entity}.
	 *
	 * @param path path to the owning property.
	 * @param resultSet the {@link ResultSet} to read from.
	 * @param identifier entity identifier.
	 * @param key primary key.
	 * @param <T>
	 * @return
	 * @deprecated use {@link #mapRow(AggregatePath, ResultSet, Identifier, Object)} instead.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	default <T> T mapRow(PersistentPropertyPathExtension path, ResultSet resultSet, Identifier identifier, Object key){
		return mapRow(path.getAggregatePath(), resultSet, identifier, key);
	};

	/**
	 * Read the current row from {@link ResultSet} to an {@link AggregatePath#getLeafEntity()} entity}.
	 *
	 * @param path path to the owning property.
	 * @param resultSet the {@link ResultSet} to read from.
	 * @param identifier entity identifier.
	 * @param key primary key.
	 * @param <T>
	 * @return
	 */
	<T> T mapRow(AggregatePath path, ResultSet resultSet, Identifier identifier, Object key);

	/**
	 * The type to be used to store this property in the database. Multidimensional arrays are unwrapped to reflect a
	 * top-level array type (e.g. {@code String[][]} returns {@code String[]}).
	 *
	 * @return a {@link Class} that is suitable for usage with JDBC drivers.
	 * @see org.springframework.data.jdbc.support.JdbcUtil#targetSqlTypeFor(Class)
	 * @since 2.0
	 */
	Class<?> getColumnType(RelationalPersistentProperty property);

	/**
	 * The SQL type constant used when using this property as a parameter for a SQL statement.
	 *
	 * @return Must not be {@code null}.
	 * @see java.sql.Types
	 * @since 2.0
	 */
	SQLType getTargetSqlType(RelationalPersistentProperty property);

	@Override
	RelationalMappingContext getMappingContext();
}
