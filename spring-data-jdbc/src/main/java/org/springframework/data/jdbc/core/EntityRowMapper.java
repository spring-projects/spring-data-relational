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
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
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
 */
public class EntityRowMapper<T> implements RowMapper<T> {

	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();

	private final RelationalPersistentEntity<T> entity;

	private final RelationalConverter converter;
	private final RelationalMappingContext context;
	private final DataAccessStrategy accessStrategy;

	public EntityRowMapper(RelationalPersistentEntity<T> entity, RelationalMappingContext context,
			RelationalConverter converter, DataAccessStrategy accessStrategy) {

		this.entity = entity;
		this.converter = converter;
		this.context = context;
		this.accessStrategy = accessStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) {

		ReadingContext<T> readingContext = new ReadingContext<>(entity, resultSet);

		T result = readingContext.createInstance();

		return entity.requiresPropertyPopulation() //
				? readingContext.populateProperties(result) //
				: result;
	}

	/**
	 * The context required for reading an entity.
	 */
	private class ReadingContext<S> {

		private final ResultSet resultSet;
		/**
		 * The path to the entity getting loaded by this context.
		 */
		private final PersistentPropertyPath<RelationalPersistentProperty> path;
		private final RelationalPersistentEntity<S> entity;
		private Object idValue;

		ReadingContext(RelationalPersistentEntity<S> entity, ResultSet resultSet,
				PersistentPropertyPath<RelationalPersistentProperty> path) {

			this.resultSet = resultSet;
			this.path = path;
			this.entity = entity;

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue = null;
			if (idProperty != null) {
				idValue = readSimplePropertyFrom(idProperty);
			}

			setId(idValue);
		}

		/**
		 * creates a {@code ReadingContext} with an empty path.
		 *
		 * @param entity the persistent entity to be loaded.
		 * @param resultSet the {@link ResultSet} to load the data from.
		 */
		ReadingContext(RelationalPersistentEntity<S> entity, ResultSet resultSet) {
			this(entity, resultSet, context.getPersistentPropertyPath("", entity.getType()));
		}

		/**
		 * creates a new {@code ReadingContext} based on the current one, but extending the path by the property provided.
		 *
		 * @param property the property provided for extending the path. Must not be {@code null}.
		 * @return a new {@code ReadingContext}
		 */
		@SuppressWarnings("unchecked")
		<R> ReadingContext<R> derive(RelationalPersistentProperty property) {

			return new ReadingContext<>( //
					(RelationalPersistentEntity<R>) context.getRequiredPersistentEntity(property.getActualType()), //
					resultSet, //
					extendPath(property) //
			);
		}

		private PersistentPropertyPath<RelationalPersistentProperty> extendPath(RelationalPersistentProperty property) {
			return context.getPersistentPropertyPath(extendedPath(property), entity.getType());
		}

		/**
		 * The prefix used to modify column_names of entities reachable via one-to-one-relationships.
		 *
		 * @return Guaranteed to be not {@code null}.
		 */
		private String prefix() {

			String dotPath = path.toDotPath();
			return dotPath == null ? "" : dotPath.replaceAll("\\.", "_") + "_";
		}

		/**
		 * creates a dot separated path string consisting of the current path plus the provided properties name.
		 *
		 * @param property the property by which to extend the path. Must not be {@code null}.
		 * @return Guaranteed to be not {@code null}.
		 */
		private String extendedPath(RelationalPersistentProperty property) {

			String nullablePath = path.toDotPath();
			return nullablePath == null ? property.getName() : nullablePath + "." + property.getName();
		}

		/**
		 * Creates an entity from the provided {@link ResultSet}.
		 *
		 * @return an instance created using the persistent constructor.
		 */
		private S createInstance() {

			return converter.createInstance(entity, parameter -> {

				String parameterName = parameter.getName();

				Assert.notNull(parameterName, "A constructor parameter name must not be null to be used with Spring Data JDBC");

				RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameterName);

				return readOrLoadProperty(property);
			});
		}

		/**
		 * Read a single value or a complete Entity from the {@link ResultSet} passed as an argument.
		 *
		 * @param property the {@link RelationalPersistentProperty} for which the value is intended. Must not be
		 *          {@code null}.
		 * @return the value read from the {@link ResultSet}. May be {@code null}.
		 */
		@Nullable
		private Object readSimplePropertyFrom(RelationalPersistentProperty property) {

			Object value = getObjectFromResultSet(property.getColumnName());
			return converter.readValue(value, property.getTypeInformation());

		}

		@Nullable
		private Object getObjectFromResultSet(String columnName) {

			String fullColumnName = prefix() + columnName;
			try {

				return resultSet.getObject(fullColumnName);
			} catch (SQLException o_O) {
				throw new MappingException(String.format("Could not read value %s from result set!", fullColumnName), o_O);
			}
		}

		private S populateProperties(S result) {

			PersistentPropertyAccessor<S> propertyAccessor = converter.getPropertyAccessor(entity, result);

			PreferredConstructor<S, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

			for (RelationalPersistentProperty property : entity) {

				if (persistenceConstructor != null && persistenceConstructor.isConstructorParameter(property)) {
					continue;
				}

				propertyAccessor.setProperty(property, readOrLoadProperty(property));
			}

			return propertyAccessor.getBean();
		}

		@Nullable
		private Object readOrLoadProperty(RelationalPersistentProperty property) {

			// TODO: this is the kind of call we need
			// accessStrategy.findAllByProperty(path, relativeRootId, keys);

			if (property.isCollectionLike()) {
				return accessStrategy.findAllByProperty(extendPath(property), idValue);
			} else if (property.isMap()) {
				return ITERABLE_OF_ENTRY_TO_MAP_CONVERTER
						.convert(accessStrategy.findAllByProperty(extendPath(property), idValue));
			} else if (property.isEntity()) {
				return readEntityFrom(property);
			} else {
				return readSimplePropertyFrom(property);
			}
		}

		@Nullable
		private <R> R readEntityFrom(RelationalPersistentProperty property) {

			ReadingContext<R> newReadingContext = derive(property);

			@SuppressWarnings("unchecked")
			RelationalPersistentEntity<R> entity = (RelationalPersistentEntity<R>) context
					.getRequiredPersistentEntity(property.getActualType());

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			// return null if either an existing id is null or the backreference is null, meaning there is no data.
			if ((idProperty != null //
					? newReadingContext.idValue //
					: newReadingContext.getObjectFromResultSet(property.getReverseColumnName()) //
			) == null) {
				return null;
			}

			R instance = newReadingContext.createInstance();

			PersistentPropertyAccessor<R> accessor = converter.getPropertyAccessor(entity, instance);

			for (RelationalPersistentProperty p : entity) {
				if (!entity.isConstructorArgument(p)) {
					accessor.setProperty(p, newReadingContext.readSimplePropertyFrom(p));
				}
			}

			return instance;
		}

		private void setId(@Nullable Object idValue) {

			Assert.isNull(this.idValue, String.format("Tried to set the idValue twice. New value %s in %s", idValue, this));

			this.idValue = idValue;
		}

	}
}
