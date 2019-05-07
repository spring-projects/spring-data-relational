/*
 * Copyright 2018-2019 the original author or authors.
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
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RelationalConverter} that uses a {@link MappingContext} to apply basic conversion of relational values to
 * property values.
 * <p>
 * Conversion is configurable by providing a customized {@link CustomConversions}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @see MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 */
public class BasicJdbcConverter extends BasicRelationalConverter implements JdbcConverter {

	private static final Logger LOG = LoggerFactory.getLogger(BasicJdbcConverter.class);
	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();
	private final JdbcTypeFactory typeFactory;

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and a
	 * {@link JdbcTypeFactory#unsupported() no-op type factory} throwing {@link UnsupportedOperationException} on type
	 * creation. Use {@link #BasicJdbcConverter(MappingContext, JdbcTypeFactory)} to convert arrays and large objects into
	 * JDBC-specific types.
	 *
	 * @param context must not be {@literal null}.
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		this(context, JdbcTypeFactory.unsupported());
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @since 1.1
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			JdbcTypeFactory typeFactory) {

		super(context);

		Assert.notNull(typeFactory, "JdbcTypeFactory must not be null");

		this.typeFactory = typeFactory;
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}, {@link CustomConversions}, and
	 * {@link JdbcTypeFactory}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @since 1.1
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions, JdbcTypeFactory typeFactory) {

		super(context, conversions);

		Assert.notNull(typeFactory, "JdbcTypeFactory must not be null");
		this.typeFactory = typeFactory;
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 * @deprecated use one of the constructors with {@link JdbcTypeFactory} parameter.
	 */
	@Deprecated
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions) {
		this(context, conversions, JdbcTypeFactory.unsupported());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#readValue(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	@Nullable
	public Object readValue(@Nullable Object value, TypeInformation<?> type) {

		if (null == value) {
			return null;
		}

		if (getConversions().hasCustomReadTarget(value.getClass(), type.getType())) {
			return getConversionService().convert(value, type.getType());
		}

		if (AggregateReference.class.isAssignableFrom(type.getType())) {

			TypeInformation<?> idType = type.getSuperTypeInformation(AggregateReference.class).getTypeArguments().get(1);

			return AggregateReference.to(readValue(value, idType));
		}

		if (value instanceof Array) {
			try {
				return readValue(((Array) value).getArray(), type);
			} catch (SQLException | ConverterNotFoundException e) {
				LOG.info("Failed to extract a value of type %s from an Array. Attempting to use standard conversions.", e);
			}
		}

		return super.readValue(value, type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#writeValue(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	@Nullable
	public Object writeValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return null;
		}

		if (AggregateReference.class.isAssignableFrom(value.getClass())) {
			return writeValue(((AggregateReference) value).getId(), type);
		}

		return super.writeValue(value, type);
	}

	private boolean canWriteAsJdbcValue(@Nullable Object value) {

		if (value == null) {
			return true;
		}

		if (AggregateReference.class.isAssignableFrom(value.getClass())) {
			return canWriteAsJdbcValue(((AggregateReference) value).getId());
		}

		RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(value.getClass());

		if (persistentEntity != null) {

			Object id = persistentEntity.getIdentifierAccessor(value).getIdentifier();
			return canWriteAsJdbcValue(id);
		}

		if (value instanceof JdbcValue) {
			return true;
		}

		Optional<Class<?>> customWriteTarget = getConversions().getCustomWriteTarget(value.getClass());
		return customWriteTarget.isPresent() && customWriteTarget.get().isAssignableFrom(JdbcValue.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.convert.JdbcConverter#writeValue(java.lang.Object, java.lang.Class, int)
	 */
	@Override
	public JdbcValue writeJdbcValue(@Nullable Object value, Class<?> columnType, int sqlType) {

		JdbcValue jdbcValue = tryToConvertToJdbcValue(value);
		if (jdbcValue != null) {
			return jdbcValue;
		}

		Object convertedValue = writeValue(value, ClassTypeInformation.from(columnType));

		if (convertedValue == null || !convertedValue.getClass().isArray()) {
			return JdbcValue.of(convertedValue, JdbcUtil.jdbcTypeFor(sqlType));
		}

		Class<?> componentType = convertedValue.getClass().getComponentType();
		if (componentType != byte.class && componentType != Byte.class) {
			return JdbcValue.of(typeFactory.createArray((Object[]) convertedValue), JDBCType.ARRAY);
		}

		if (componentType == Byte.class) {
			convertedValue = ArrayUtil.toPrimitiveByteArray((Byte[]) convertedValue);
		}

		return JdbcValue.of(convertedValue, JDBCType.BINARY);
	}

	@Nullable
	private JdbcValue tryToConvertToJdbcValue(@Nullable Object value) {

		if (canWriteAsJdbcValue(value)) {
			return (JdbcValue) writeValue(value, ClassTypeInformation.from(JdbcValue.class));
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	@Override
	public <T> T mapRow(RelationalPersistentEntity<T> entity, DataAccessStrategy accessStrategy, ResultSet resultSet) {
		return new ReadingContext<T>(entity, accessStrategy, resultSet).mapRow();
	}

	private class ReadingContext<T> {

		private final RelationalPersistentEntity<T> entity;
		private final RelationalPersistentProperty idProperty;

		private final ResultSet resultSet;
		PersistentPropertyPathExtension path;
		private final DataAccessStrategy accessStrategy;

		ReadingContext(RelationalPersistentEntity<T> entity, DataAccessStrategy accessStrategy, ResultSet resultSet) {

			this.entity = entity;
			this.idProperty = entity.getIdProperty();
			this.accessStrategy = accessStrategy;
			this.resultSet = resultSet;
			this.path = new PersistentPropertyPathExtension(
					(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>) getMappingContext(), entity);
		}

		public ReadingContext(RelationalPersistentEntity<T> entity, DataAccessStrategy accessStrategy, ResultSet resultSet,
				PersistentPropertyPathExtension path) {

			this.entity = entity;
			this.idProperty = entity.getIdProperty();
			this.accessStrategy = accessStrategy;
			this.resultSet = resultSet;
			this.path = path;
		}

		private ReadingContext<?> extendBy(RelationalPersistentProperty property) {
			return new ReadingContext<>(entity, accessStrategy, resultSet, path.extendBy(property));
		}

		T mapRow() {

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue = null;
			if (idProperty != null) {
				idValue = readFrom(idProperty);
			}

			T result = createInstanceInternal(entity, idValue);

			return entity.requiresPropertyPopulation() //
					? populateProperties(result) //
					: result;
		}

		private T populateProperties(T result) {

			PersistentPropertyAccessor<T> propertyAccessor = getPropertyAccessor(entity, result);

			Object id = idProperty == null ? null : readFrom(idProperty);

			PreferredConstructor<T, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

			for (RelationalPersistentProperty property : entity) {

				if (persistenceConstructor != null && persistenceConstructor.isConstructorParameter(property)) {
					continue;
				}

				propertyAccessor.setProperty(property, readOrLoadProperty(id, property));
			}

			return propertyAccessor.getBean();
		}

		@Nullable
		private Object readOrLoadProperty(@Nullable Object id, RelationalPersistentProperty property) {

			if (property.isCollectionLike() && property.isEntity() && id != null) {
				return accessStrategy.findAllByProperty(id, property);
			} else if (property.isMap() && id != null) {
				return ITERABLE_OF_ENTRY_TO_MAP_CONVERTER.convert(accessStrategy.findAllByProperty(id, property));
			} else if (property.isEmbedded()) {
				return readEmbeddedEntityFrom(id, property);
			} else {
				return readFrom(property);
			}
		}

		/**
		 * Read a single value or a complete Entity from the {@link ResultSet} passed as an argument.
		 *
		 * @param property the {@link RelationalPersistentProperty} for which the value is intended. Must not be
		 *          {@code null}.
		 * @return the value read from the {@link ResultSet}. May be {@code null}.
		 */
		@Nullable
		private Object readFrom(RelationalPersistentProperty property) {

			if (property.isEntity()) {
				return readEntityFrom(property, path);
			}

			Object value = getObjectFromResultSet(path.extendBy(property).getColumnAlias());
			return readValue(value, property.getTypeInformation());

		}

		@SuppressWarnings("unchecked")
		private Object readEmbeddedEntityFrom(@Nullable Object id, RelationalPersistentProperty property) {

			ReadingContext newContext = extendBy(property);

			RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(property.getActualType());

			Object instance = newContext.createInstanceInternal(entity, null);

			PersistentPropertyAccessor<?> accessor = getPropertyAccessor((PersistentEntity<Object, ?>) entity, instance);

			for (RelationalPersistentProperty p : entity) {
				accessor.setProperty(p, newContext.readOrLoadProperty(id, p));
			}

			return instance;
		}

		@Nullable
		private <S> S readEntityFrom(RelationalPersistentProperty property, PersistentPropertyPathExtension path) {

			ReadingContext<?> newContext = extendBy(property);

			RelationalPersistentEntity<S> entity = (RelationalPersistentEntity<S>) getMappingContext()
					.getRequiredPersistentEntity(property.getActualType());

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue = null;

			if (idProperty != null) {
				idValue = newContext.readFrom(idProperty);
			}

			if ((idProperty != null //
					? idValue //
					: newContext.getObjectFromResultSet(path.extendBy(property).getReverseColumnNameAlias()) //
			) == null) {
				return null;
			}

			S instance = newContext.createInstanceInternal(entity, idValue);

			PersistentPropertyAccessor<S> accessor = getPropertyAccessor(entity, instance);

			for (RelationalPersistentProperty p : entity) {
				accessor.setProperty(p, newContext.readOrLoadProperty(idValue, p));
			}

			return instance;
		}

		@Nullable
		private Object getObjectFromResultSet(String backreferenceName) {

			try {
				return resultSet.getObject(backreferenceName);
			} catch (SQLException o_O) {
				throw new MappingException(String.format("Could not read value %s from result set!", backreferenceName), o_O);
			}
		}

		private <S> S createInstanceInternal(RelationalPersistentEntity<S> entity, @Nullable Object idValue) {

			return createInstance(entity, parameter -> {

				String parameterName = parameter.getName();

				Assert.notNull(parameterName, "A constructor parameter name must not be null to be used with Spring Data JDBC");

				RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameterName);

				return readOrLoadProperty(idValue, property);
			});
		}

	}
}
