/*
 * Copyright 2018-2020 the original author or authors.
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
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.data.relational.domain.IdentifierProcessing;
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
 * @author Christoph Strobl
 * @author Myeonghyeon Lee
 * @since 1.1
 * @see MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 */
public class BasicJdbcConverter extends BasicRelationalConverter implements JdbcConverter {

	private static final Logger LOG = LoggerFactory.getLogger(BasicJdbcConverter.class);
	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();

	private final JdbcTypeFactory typeFactory;
	private final IdentifierProcessing identifierProcessing = HsqlDbDialect.INSTANCE.getIdentifierProcessing();

	private RelationResolver relationResolver;

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and a
	 * {@link JdbcTypeFactory#unsupported() no-op type factory} throwing {@link UnsupportedOperationException} on type
	 * creation. Use {@link #BasicJdbcConverter(MappingContext, RelationResolver, CustomConversions, JdbcTypeFactory)}
	 * (MappingContext, RelationResolver, JdbcTypeFactory)} to convert arrays and large objects into JDBC-specific types.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			RelationResolver relationResolver) {

		super(context);

		Assert.notNull(relationResolver, "RelationResolver must not be null");

		this.relationResolver = relationResolver;
		this.typeFactory = JdbcTypeFactory.unsupported();
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @since 1.1
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			RelationResolver relationResolver, CustomConversions conversions, JdbcTypeFactory typeFactory) {

		super(context, conversions);

		Assert.notNull(typeFactory, "JdbcTypeFactory must not be null");
		Assert.notNull(relationResolver, "RelationResolver must not be null");

		this.relationResolver = relationResolver;
		this.typeFactory = typeFactory;
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

			if (type.getType().isAssignableFrom(value.getClass())) {
				return value;
			}

			return readAggregateReference(value, type);
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

	@SuppressWarnings("ConstantConditions")
	private Object readAggregateReference(@Nullable Object value, TypeInformation<?> type) {

		TypeInformation<?> idType = type.getSuperTypeInformation(AggregateReference.class).getTypeArguments().get(1);

		return AggregateReference.to(readValue(value, idType));
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> T mapRow(RelationalPersistentEntity<T> entity, ResultSet resultSet, Object key) {
		return new ReadingContext<T>(
				new PersistentPropertyPathExtension(
						(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>) getMappingContext(), entity),
				resultSet, Identifier.empty(), key).mapRow();
	}

	@Override
	public <T> T mapRow(PersistentPropertyPathExtension path, ResultSet resultSet, Identifier identifier, Object key) {
		return new ReadingContext<T>(path, resultSet, identifier, key).mapRow();
	}

	private class ReadingContext<T> {

		private final RelationalPersistentEntity<T> entity;

		private final ResultSet resultSet;
		private final PersistentPropertyPathExtension rootPath;
		private final PersistentPropertyPathExtension path;
		private final Identifier identifier;
		private final Object key;

		@SuppressWarnings("unchecked")
		private ReadingContext(PersistentPropertyPathExtension rootPath, ResultSet resultSet, Identifier identifier,
				Object key) {

			RelationalPersistentEntity<T> entity = (RelationalPersistentEntity<T>) rootPath.getLeafEntity();

			Assert.notNull(entity, "The rootPath must point to an entity.");

			this.entity = entity;
			this.resultSet = resultSet;
			this.rootPath = rootPath;
			this.path = new PersistentPropertyPathExtension(
					(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>) getMappingContext(),
					this.entity);
			this.identifier = identifier;
			this.key = key;
		}

		private ReadingContext(RelationalPersistentEntity<T> entity, ResultSet resultSet,
				PersistentPropertyPathExtension rootPath, PersistentPropertyPathExtension path, Identifier identifier,
				Object key) {

			this.entity = entity;
			this.resultSet = resultSet;
			this.rootPath = rootPath;
			this.path = path;
			this.identifier = identifier;
			this.key = key;
		}

		private <S> ReadingContext<S> extendBy(RelationalPersistentProperty property) {
			return new ReadingContext<>(
					(RelationalPersistentEntity<S>) getMappingContext().getRequiredPersistentEntity(property.getActualType()),
					resultSet, rootPath.extendBy(property), path.extendBy(property), identifier, key);
		}

		T mapRow() {

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue = idProperty == null ? null : readFrom(idProperty);

			return createInstanceInternal(idValue);
		}

		private T populateProperties(T instance, @Nullable Object idValue) {

			PersistentPropertyAccessor<T> propertyAccessor = getPropertyAccessor(entity, instance);

			PreferredConstructor<T, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

			for (RelationalPersistentProperty property : entity) {

				if (persistenceConstructor != null && persistenceConstructor.isConstructorParameter(property)) {
					continue;
				}

				propertyAccessor.setProperty(property, readOrLoadProperty(idValue, property));
			}

			return propertyAccessor.getBean();
		}

		@Nullable
		private Object readOrLoadProperty(@Nullable Object id, RelationalPersistentProperty property) {

			if ((property.isCollectionLike() && property.isEntity()) || property.isMap()) {

				Iterable<Object> allByPath = resolveRelation(id, property);

				return property.isMap() //
						? ITERABLE_OF_ENTRY_TO_MAP_CONVERTER.convert(allByPath) //
						: allByPath;

			} else if (property.isEmbedded()) {
				return readEmbeddedEntityFrom(id, property);
			} else {
				return readFrom(property);
			}
		}

		private Iterable<Object> resolveRelation(@Nullable Object id, RelationalPersistentProperty property) {

			Identifier identifier = id == null //
					? this.identifier.withPart(rootPath.getQualifierColumn(), key, Object.class) //
					: Identifier.of(rootPath.extendBy(property).getReverseColumnName(), id, Object.class);

			PersistentPropertyPath<RelationalPersistentProperty> propertyPath = path.extendBy(property)
					.getRequiredPersistentPropertyPath();

			return relationResolver.findAllByPath(identifier, propertyPath);
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

			Object value = getObjectFromResultSet(
					path.extendBy(property).getColumnAlias().getReference(identifierProcessing));
			return readValue(value, property.getTypeInformation());
		}

		@Nullable
		private Object readEmbeddedEntityFrom(@Nullable Object idValue, RelationalPersistentProperty property) {

			ReadingContext<?> newContext = extendBy(property);

			if (shouldCreateEmptyEmbeddedInstance(property) || newContext.hasInstanceValues(idValue)) {
				return newContext.createInstanceInternal(idValue);
			}

			return null;
		}

		private boolean shouldCreateEmptyEmbeddedInstance(RelationalPersistentProperty property) {
			return OnEmpty.USE_EMPTY.equals(property.findAnnotation(Embedded.class).onEmpty());
		}

		private boolean hasInstanceValues(@Nullable Object idValue) {

			RelationalPersistentEntity<?> persistentEntity = path.getLeafEntity();

			Assert.state(persistentEntity != null, "Entity must not be null");

			for (RelationalPersistentProperty embeddedProperty : persistentEntity) {

				// if the embedded contains Lists, Sets or Maps we consider it non-empty
				if (embeddedProperty.isQualified() || embeddedProperty.isReference()) {
					return true;
				}

				if (readOrLoadProperty(idValue, embeddedProperty) != null) {
					return true;
				}
			}

			return false;
		}

		@Nullable
		@SuppressWarnings("unchecked")
		private Object readEntityFrom(RelationalPersistentProperty property, PersistentPropertyPathExtension path) {

			ReadingContext<?> newContext = extendBy(property);
			RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(property.getActualType());
			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue;

			if (idProperty != null) {
				idValue = newContext.readFrom(idProperty);
			} else {
				idValue = newContext.getObjectFromResultSet(
						path.extendBy(property).getReverseColumnNameAlias().getReference(identifierProcessing));
			}

			if (idValue == null) {
				return null;
			}

			return newContext.createInstanceInternal(idValue);
		}

		@Nullable
		private Object getObjectFromResultSet(String backreferenceName) {

			try {
				return resultSet.getObject(backreferenceName);
			} catch (SQLException o_O) {
				throw new MappingException(String.format("Could not read value %s from result set!", backreferenceName), o_O);
			}
		}

		private T createInstanceInternal(@Nullable Object idValue) {

			T instance = createInstance(entity, parameter -> {

				String parameterName = parameter.getName();

				Assert.notNull(parameterName, "A constructor parameter name must not be null to be used with Spring Data JDBC");

				RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameterName);

				return readOrLoadProperty(idValue, property);
			});
			return populateProperties(instance, idValue);
		}

	}
}
