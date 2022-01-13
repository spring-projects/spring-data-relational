/*
 * Copyright 2018-2021 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
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
 * @see MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 * @since 1.1
 */
public class BasicJdbcConverter extends BasicRelationalConverter implements JdbcConverter, ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(BasicJdbcConverter.class);
	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();

	private final JdbcTypeFactory typeFactory;
	private final IdentifierProcessing identifierProcessing;

	private final RelationResolver relationResolver;
	private SpELContext spELContext;

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and a
	 * {@link JdbcTypeFactory#unsupported() no-op type factory} throwing {@link UnsupportedOperationException} on type
	 * creation. Use
	 * {@link #BasicJdbcConverter(MappingContext, RelationResolver, CustomConversions, JdbcTypeFactory, IdentifierProcessing)}
	 * (MappingContext, RelationResolver, JdbcTypeFactory)} to convert arrays and large objects into JDBC-specific types.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			RelationResolver relationResolver) {

		super(context, new JdbcCustomConversions());

		Assert.notNull(relationResolver, "RelationResolver must not be null");

		this.typeFactory = JdbcTypeFactory.unsupported();
		this.identifierProcessing = IdentifierProcessing.ANSI;
		this.relationResolver = relationResolver;
		this.spELContext = new SpELContext(ResultSetAccessorPropertyAccessor.INSTANCE);
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @param identifierProcessing must not be {@literal null}
	 * @since 2.0
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			RelationResolver relationResolver, CustomConversions conversions, JdbcTypeFactory typeFactory,
			IdentifierProcessing identifierProcessing) {

		super(context, conversions);

		Assert.notNull(typeFactory, "JdbcTypeFactory must not be null");
		Assert.notNull(relationResolver, "RelationResolver must not be null");
		Assert.notNull(identifierProcessing, "IdentifierProcessing must not be null");

		this.typeFactory = typeFactory;
		this.identifierProcessing = identifierProcessing;
		this.relationResolver = relationResolver;
		this.spELContext = new SpELContext(ResultSetAccessorPropertyAccessor.INSTANCE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	@Nullable
	private Class<?> getEntityColumnType(Class<?> type) {

		RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(type);

		if (persistentEntity == null) {
			return null;
		}

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();

		if (idProperty == null) {
			return null;
		}
		return getColumnType(idProperty);
	}

	private Class<?> getReferenceColumnType(RelationalPersistentProperty property) {

		Class<?> componentType = property.getTypeInformation().getRequiredComponentType().getType();
		RelationalPersistentEntity<?> referencedEntity = getMappingContext().getRequiredPersistentEntity(componentType);

		return getColumnType(referencedEntity.getRequiredIdProperty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.convert.JdbcConverter#getSqlType(org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public int getSqlType(RelationalPersistentProperty property) {
		return JdbcUtil.sqlTypeFor(getColumnType(property));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.convert.JdbcConverter#getColumnType(org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public Class<?> getColumnType(RelationalPersistentProperty property) {
		return doGetColumnType(property);
	}

	private Class<?> doGetColumnType(RelationalPersistentProperty property) {

		if (property.isAssociation()) {
			return getReferenceColumnType(property);
		}

		if (property.isEntity()) {
			Class<?> columnType = getEntityColumnType(property.getActualType());

			if (columnType != null) {
				return columnType;
			}
		}

		Class<?> componentColumnType = JdbcColumnTypes.INSTANCE.resolvePrimitiveType(property.getActualType());

		while (componentColumnType.isArray()) {
			componentColumnType = componentColumnType.getComponentType();
		}

		if (property.isCollectionLike() && !property.isEntity()) {
			return java.lang.reflect.Array.newInstance(componentColumnType, 0).getClass();
		}

		return componentColumnType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#readValue(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	@Nullable
	public Object readValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return value;
		}

		if (getConversions().hasCustomReadTarget(value.getClass(), type.getType())) {

			TypeDescriptor sourceDescriptor = TypeDescriptor.valueOf(value.getClass());
			TypeDescriptor targetDescriptor = createTypeDescriptor(type);

			return getConversionService().convert(value, sourceDescriptor, targetDescriptor);
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

			Object[] objectArray = requireObjectArray(convertedValue);
			return JdbcValue.of(typeFactory.createArray(objectArray), JDBCType.ARRAY);
		}

		if (componentType == Byte.class) {
			convertedValue = ArrayUtils.toPrimitive((Byte[]) convertedValue);
		}

		return JdbcValue.of(convertedValue, JDBCType.BINARY);
	}

	@Nullable
	private JdbcValue tryToConvertToJdbcValue(@Nullable Object value) {

		if (canWriteAsJdbcValue(value)) {

			Object converted = writeValue(value, ClassTypeInformation.from(JdbcValue.class));
			if (converted instanceof JdbcValue) {
				return (JdbcValue) converted;
			}
		}

		return null;
	}

	@Override
	public <T> T mapRow(RelationalPersistentEntity<T> entity, ResultSet resultSet, Object key) {
		return new ReadingContext<T>(new PersistentPropertyPathExtension(getMappingContext(), entity),
				new ResultSetAccessor(resultSet), Identifier.empty(), key).mapRow();
	}

	@Override
	public <T> T mapRow(PersistentPropertyPathExtension path, ResultSet resultSet, Identifier identifier, Object key) {
		return new ReadingContext<T>(path, new ResultSetAccessor(resultSet), identifier, key).mapRow();
	}

	static Object[] requireObjectArray(Object source) {

		Assert.isTrue(source.getClass().isArray(), "Source object is not an array");

		Class<?> componentType = source.getClass().getComponentType();

		if (componentType.isPrimitive()) {
			if (componentType == boolean.class) {
				return ArrayUtils.toObject((boolean[]) source);
			}
			if (componentType == byte.class) {
				return ArrayUtils.toObject((byte[]) source);
			}
			if (componentType == char.class) {
				return ArrayUtils.toObject((char[]) source);
			}
			if (componentType == double.class) {
				return ArrayUtils.toObject((double[]) source);
			}
			if (componentType == float.class) {
				return ArrayUtils.toObject((float[]) source);
			}
			if (componentType == int.class) {
				return ArrayUtils.toObject((int[]) source);
			}
			if (componentType == long.class) {
				return ArrayUtils.toObject((long[]) source);
			}
			if (componentType == short.class) {
				return ArrayUtils.toObject((short[]) source);
			}

			throw new IllegalArgumentException("Unsupported component type: " + componentType);
		}
		return (Object[]) source;
	}

	private class ReadingContext<T> {

		private final RelationalPersistentEntity<T> entity;

		private final PersistentPropertyPathExtension rootPath;
		private final PersistentPropertyPathExtension path;
		private final Identifier identifier;
		private final Object key;

		private final JdbcPropertyValueProvider propertyValueProvider;
		private final JdbcBackReferencePropertyValueProvider backReferencePropertyValueProvider;
		private final ResultSetAccessor accessor;

		@SuppressWarnings("unchecked")
		private ReadingContext(PersistentPropertyPathExtension rootPath, ResultSetAccessor accessor, Identifier identifier,
				Object key) {
			RelationalPersistentEntity<T> entity = (RelationalPersistentEntity<T>) rootPath.getLeafEntity();

			Assert.notNull(entity, "The rootPath must point to an entity.");

			this.entity = entity;
			this.rootPath = rootPath;
			this.path = new PersistentPropertyPathExtension(getMappingContext(), this.entity);
			this.identifier = identifier;
			this.key = key;
			this.propertyValueProvider = new JdbcPropertyValueProvider(identifierProcessing, path, accessor);
			this.backReferencePropertyValueProvider = new JdbcBackReferencePropertyValueProvider(identifierProcessing, path,
					accessor);
			this.accessor = accessor;
		}

		private ReadingContext(RelationalPersistentEntity<T> entity, PersistentPropertyPathExtension rootPath,
				PersistentPropertyPathExtension path, Identifier identifier, Object key,
				JdbcPropertyValueProvider propertyValueProvider,
				JdbcBackReferencePropertyValueProvider backReferencePropertyValueProvider, ResultSetAccessor accessor) {
			this.entity = entity;
			this.rootPath = rootPath;
			this.path = path;
			this.identifier = identifier;
			this.key = key;
			this.propertyValueProvider = propertyValueProvider;
			this.backReferencePropertyValueProvider = backReferencePropertyValueProvider;
			this.accessor = accessor;
		}

		private <S> ReadingContext<S> extendBy(RelationalPersistentProperty property) {
			return new ReadingContext<>(
					(RelationalPersistentEntity<S>) getMappingContext().getRequiredPersistentEntity(property.getActualType()),
					rootPath.extendBy(property), path.extendBy(property), identifier, key,
					propertyValueProvider.extendBy(property), backReferencePropertyValueProvider.extendBy(property), accessor);
		}

		T mapRow() {

			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue = idProperty == null ? null : readFrom(idProperty);

			return createInstanceInternal(idValue);
		}

		private T populateProperties(T instance, @Nullable Object idValue) {

			PersistentPropertyAccessor<T> propertyAccessor = getPropertyAccessor(entity, instance);
			PreferredConstructor<T, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

			entity.doWithAll(property -> {

				if (persistenceConstructor != null && persistenceConstructor.isConstructorParameter(property)) {
					return;
				}

				// skip absent simple properties
				if (isSimpleProperty(property)) {

					if (!propertyValueProvider.hasProperty(property)) {
						return;
					}
				}

				Object value = readOrLoadProperty(idValue, property);
				propertyAccessor.setProperty(property, value);
			});

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

			PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath = path.extendBy(property)
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
				return readEntityFrom(property);
			}

			Object value = propertyValueProvider.getPropertyValue(property);
			return value != null ? readValue(value, property.getTypeInformation()) : null;
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
			return property.shouldCreateEmptyEmbedded();
		}

		private boolean hasInstanceValues(@Nullable Object idValue) {

			RelationalPersistentEntity<?> persistentEntity = path.getLeafEntity();

			Assert.state(persistentEntity != null, "Entity must not be null");

			for (RelationalPersistentProperty embeddedProperty : persistentEntity) {

				// if the embedded contains Lists, Sets or Maps we consider it non-empty
				if (embeddedProperty.isQualified() || embeddedProperty.isAssociation()) {
					return true;
				}

				Object value = readOrLoadProperty(idValue, embeddedProperty);
				if (value != null) {
					return true;
				}
			}

			return false;
		}

		@Nullable
		@SuppressWarnings("unchecked")
		private Object readEntityFrom(RelationalPersistentProperty property) {

			ReadingContext<?> newContext = extendBy(property);
			RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(property.getActualType());
			RelationalPersistentProperty idProperty = entity.getIdProperty();

			Object idValue;

			if (idProperty != null) {
				idValue = newContext.readFrom(idProperty);
			} else {
				idValue = backReferencePropertyValueProvider.getPropertyValue(property);
			}

			if (idValue == null) {
				return null;
			}

			return newContext.createInstanceInternal(idValue);
		}

		private T createInstanceInternal(@Nullable Object idValue) {

			PreferredConstructor<T, RelationalPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();
			ParameterValueProvider<RelationalPersistentProperty> provider;

			if (persistenceConstructor != null && persistenceConstructor.hasParameters()) {

				SpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(accessor, spELContext);
				provider = new SpELExpressionParameterValueProvider<>(expressionEvaluator, getConversionService(),
						new ResultSetParameterValueProvider(idValue, entity));
			} else {
				provider = NoOpParameterValueProvider.INSTANCE;
			}

			T instance = createInstance(entity, provider::getParameterValue);

			return entity.requiresPropertyPopulation() ? populateProperties(instance, idValue) : instance;
		}

		/**
		 * {@link ParameterValueProvider} that reads a simple property or materializes an object for a
		 * {@link RelationalPersistentProperty}.
		 *
		 * @see #readOrLoadProperty(Object, RelationalPersistentProperty)
		 * @since 2.1
		 */
		private class ResultSetParameterValueProvider implements ParameterValueProvider<RelationalPersistentProperty> {

			private final @Nullable Object idValue;
			private final RelationalPersistentEntity<?> entity;

			public ResultSetParameterValueProvider(@Nullable Object idValue, RelationalPersistentEntity<?> entity) {
				this.idValue = idValue;
				this.entity = entity;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
			 */
			@Override
			@Nullable
			public <T> T getParameterValue(PreferredConstructor.Parameter<T, RelationalPersistentProperty> parameter) {

				String parameterName = parameter.getName();

				Assert.notNull(parameterName, "A constructor parameter name must not be null to be used with Spring Data JDBC");

				RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameterName);
				return (T) readOrLoadProperty(idValue, property);
			}
		}
	}

	private boolean isSimpleProperty(RelationalPersistentProperty property) {
		return !property.isCollectionLike() && !property.isEntity() && !property.isMap() && !property.isEmbedded();
	}

	enum NoOpParameterValueProvider implements ParameterValueProvider<RelationalPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getParameterValue(PreferredConstructor.Parameter<T, RelationalPersistentProperty> parameter) {
			return null;
		}
	}

}
