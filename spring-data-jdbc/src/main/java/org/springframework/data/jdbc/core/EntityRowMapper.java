/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.PersistentPropertyPathExtension;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Maps a {@link ResultSet} to an entity of type {@code T}, including entities referenced. This {@link RowMapper} might
 * trigger additional SQL statements in order to load other members of the same aggregate.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Bastian Wilhelm
 */
public class EntityRowMapper<T> implements RowMapper<T> {

	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();

	private final RelationalPersistentEntity<T> entity;

	private final RelationalConverter converter;
	private final RelationalMappingContext context;
	private final DataAccessStrategy accessStrategy;
	private final RelationalPersistentProperty idProperty;

	public EntityRowMapper(RelationalPersistentEntity<T> entity, RelationalMappingContext context,
			RelationalConverter converter, DataAccessStrategy accessStrategy) {

		this.entity = entity;
		this.converter = converter;
		this.context = context;
		this.accessStrategy = accessStrategy;
		this.idProperty = entity.getIdProperty();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) {

		PersistentPropertyPathExtension path = new PersistentPropertyPathExtension(context, entity);

		RelationalPersistentProperty idProperty = entity.getIdProperty();

		Object idValue = null;
		if (idProperty != null) {
			idValue = readFrom(resultSet, idProperty, path);
		}

		T result = createInstance(entity, resultSet, idValue, path);

		return entity.requiresPropertyPopulation() //
				? populateProperties(result, resultSet, path) //
				: result;
	}

	private T populateProperties(T result, ResultSet resultSet, PersistentPropertyPathExtension path) {

		PersistentPropertyAccessor<T> propertyAccessor = converter.getPropertyAccessor(entity, result);

		Object id = idProperty == null ? null : readFrom(resultSet, idProperty, path);

		PreferredConstructor<T, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

		for (RelationalPersistentProperty property : entity) {

			if (persistenceConstructor != null && persistenceConstructor.isConstructorParameter(property)) {
				continue;
			}

			propertyAccessor.setProperty(property, readOrLoadProperty(resultSet, id, property, path));
		}

		return propertyAccessor.getBean();
	}

	@Nullable
	private Object readOrLoadProperty(ResultSet resultSet, @Nullable Object id, RelationalPersistentProperty property,
			PersistentPropertyPathExtension path) {

		if (property.isCollectionLike() && property.isEntity() && id != null) {
			return accessStrategy.findAllByProperty(id, property);
		} else if (property.isMap() && id != null) {
			return ITERABLE_OF_ENTRY_TO_MAP_CONVERTER.convert(accessStrategy.findAllByProperty(id, property));
		} else if (property.isEmbedded()) {
			return readEmbeddedEntityFrom(resultSet, id, property, path);
		} else {
			return readFrom(resultSet, property, path);
		}
	}

	/**
	 * Read a single value or a complete Entity from the {@link ResultSet} passed as an argument.
	 *
	 * @param resultSet the {@link ResultSet} to extract the value from. Must not be {@code null}.
	 * @param property the {@link RelationalPersistentProperty} for which the value is intended. Must not be {@code null}.
	 * @param path
	 * @return the value read from the {@link ResultSet}. May be {@code null}.
	 */
	@Nullable
	private Object readFrom(ResultSet resultSet, RelationalPersistentProperty property,
			PersistentPropertyPathExtension path) {

		if (property.isEntity()) {
			return readEntityFrom(resultSet, property, path);
		}

		Object value = getObjectFromResultSet(resultSet, path.extendBy(property).getColumnAlias());
		return converter.readValue(value, property.getTypeInformation());

	}

	private Object readEmbeddedEntityFrom(ResultSet rs, @Nullable Object id, RelationalPersistentProperty property,
			PersistentPropertyPathExtension path) {

		PersistentPropertyPathExtension newPath = path.extendBy(property);

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(property.getActualType());

		Object instance = createInstance(entity, rs, null, newPath);

		@SuppressWarnings("unchecked")
		PersistentPropertyAccessor<?> accessor = converter.getPropertyAccessor((PersistentEntity<Object, ?>) entity,
				instance);

		for (RelationalPersistentProperty p : entity) {
			accessor.setProperty(p, readOrLoadProperty(rs, id, p, newPath));
		}

		return instance;
	}

	@Nullable
	private <S> S readEntityFrom(ResultSet rs, RelationalPersistentProperty property,
			PersistentPropertyPathExtension path) {

		PersistentPropertyPathExtension newPath = path.extendBy(property);

		@SuppressWarnings("unchecked")
		RelationalPersistentEntity<S> entity = (RelationalPersistentEntity<S>) context
				.getRequiredPersistentEntity(property.getActualType());

		RelationalPersistentProperty idProperty = entity.getIdProperty();

		Object idValue = null;

		if (idProperty != null) {
			idValue = readFrom(rs, idProperty, newPath);
		}

		if ((idProperty != null //
				? idValue //
				: getObjectFromResultSet(rs, newPath.getReverseColumnNameAlias()) //
		) == null) {
			return null;
		}

		S instance = createInstance(entity, rs, idValue, newPath);

		PersistentPropertyAccessor<S> accessor = converter.getPropertyAccessor(entity, instance);

		for (RelationalPersistentProperty p : entity) {
			accessor.setProperty(p, readOrLoadProperty(rs, idValue, p, newPath));
		}

		return instance;
	}

	@Nullable
	private Object getObjectFromResultSet(ResultSet rs, String backreferenceName) {

		try {
			return rs.getObject(backreferenceName);
		} catch (SQLException o_O) {
			throw new MappingException(String.format("Could not read value %s from result set!", backreferenceName), o_O);
		}
	}

	private <S> S createInstance(RelationalPersistentEntity<S> entity, ResultSet rs, @Nullable Object idValue,
			PersistentPropertyPathExtension path) {

		return converter.createInstance(entity, parameter -> {

			String parameterName = parameter.getName();

			Assert.notNull(parameterName, "A constructor parameter name must not be null to be used with Spring Data JDBC");

			RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameterName);

			return readOrLoadProperty(rs, idValue, property, path);
		});
	}
}
