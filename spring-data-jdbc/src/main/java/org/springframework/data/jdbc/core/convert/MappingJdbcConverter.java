/*
 * Copyright 2023-2024 the original author or authors.
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
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.relational.core.conversion.MappingRelationalConverter;
import org.springframework.data.relational.core.conversion.ObjectPath;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.conversion.RowDocumentAccessor;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RelationalConverter} that uses a {@link MappingContext} to apply conversion of relational values to property
 * values.
 * <p>
 * Conversion is configurable by providing a customized {@link CustomConversions}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Christoph Strobl
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @see MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 * @since 3.2
 */
public class MappingJdbcConverter extends MappingRelationalConverter implements JdbcConverter, ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(MappingJdbcConverter.class);
	private static final Converter<Iterable<?>, Map<?, ?>> ITERABLE_OF_ENTRY_TO_MAP_CONVERTER = new IterableOfEntryToMapConverter();

	private final JdbcTypeFactory typeFactory;
	private final RelationResolver relationResolver;

	/**
	 * Creates a new {@link MappingJdbcConverter} given {@link MappingContext} and a {@link JdbcTypeFactory#unsupported()
	 * no-op type factory} throwing {@link UnsupportedOperationException} on type creation. Use
	 * {@link #MappingJdbcConverter(RelationalMappingContext, RelationResolver, CustomConversions, JdbcTypeFactory)}
	 * (MappingContext, RelationResolver, JdbcTypeFactory)} to convert arrays and large objects into JDBC-specific types.
	 *
	 * @param context          must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 */
	public MappingJdbcConverter(RelationalMappingContext context, RelationResolver relationResolver) {

		super(context, new JdbcCustomConversions());

		Assert.notNull(relationResolver, "RelationResolver must not be null");

		this.typeFactory = JdbcTypeFactory.unsupported();
		this.relationResolver = relationResolver;

		registerAggregateReferenceConverters();
	}

	/**
	 * Creates a new {@link MappingJdbcConverter} given {@link MappingContext}.
	 *
	 * @param context          must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 * @param typeFactory      must not be {@literal null}
	 */
	public MappingJdbcConverter(RelationalMappingContext context, RelationResolver relationResolver,
								CustomConversions conversions, JdbcTypeFactory typeFactory) {

		super(context, conversions);

		Assert.notNull(typeFactory, "JdbcTypeFactory must not be null");
		Assert.notNull(relationResolver, "RelationResolver must not be null");

		this.typeFactory = typeFactory;
		this.relationResolver = relationResolver;

		registerAggregateReferenceConverters();
	}

	private void registerAggregateReferenceConverters() {

		ConverterRegistry registry = (ConverterRegistry) getConversionService();
		AggregateReferenceConverters.getConvertersToRegister(getConversionService()).forEach(registry::addConverter);
	}

	@Nullable
	private Class<?> getEntityColumnType(TypeInformation<?> type) {

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

	@Override
	public SQLType getTargetSqlType(RelationalPersistentProperty property) {
		return JdbcUtil.targetSqlTypeFor(getColumnType(property));
	}

	@Override
	public Class<?> getColumnType(RelationalPersistentProperty property) {
		return doGetColumnType(property);
	}

	private Class<?> doGetColumnType(RelationalPersistentProperty property) {

		if (property.isAssociation()) {
			return getReferenceColumnType(property);
		}

		if (property.isEntity()) {
			Class<?> columnType = getEntityColumnType(property.getTypeInformation().getActualType());

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

	@Override
	@Nullable
	public Object readValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return value;
		}

		if (value instanceof Array array) {
			try {
				return super.readValue(array.getArray(), type);
			} catch (SQLException | ConverterNotFoundException e) {
				LOG.info("Failed to extract a value of type %s from an Array; Attempting to use standard conversions", e);
			}
		}

		return super.readValue(value, type);
	}

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

		if (value instanceof AggregateReference aggregateReference) {
			return canWriteAsJdbcValue(aggregateReference.getId());
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

	@Override
	public JdbcValue writeJdbcValue(@Nullable Object value, TypeInformation<?> columnType, SQLType sqlType) {

		TypeInformation<?> targetType = canWriteAsJdbcValue(value) ? TypeInformation.of(JdbcValue.class) : columnType;
		Object convertedValue = writeValue(value, targetType);

		if (convertedValue instanceof JdbcValue result) {
			return result;
		}

		if (convertedValue == null || !convertedValue.getClass().isArray()) {
			return JdbcValue.of(convertedValue, sqlType);
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

	@SuppressWarnings("unchecked")
	@Override
	public <R> R readAndResolve(TypeInformation<R> type, RowDocument source, Identifier identifier) {

		RelationalPersistentEntity<R> entity = (RelationalPersistentEntity<R>) getMappingContext()
				.getRequiredPersistentEntity(type);
		AggregatePath path = getMappingContext().getAggregatePath(entity);
		Identifier identifierToUse = ResolvingRelationalPropertyValueProvider.potentiallyAppendIdentifier(identifier,
				entity, it -> source.get(it.getColumnName().getReference()));
		ResolvingConversionContext context = new ResolvingConversionContext(getConversionContext(ObjectPath.ROOT), path,
				identifierToUse);

		return readAggregate(context, source, entity.getTypeInformation());
	}

	@Override
	protected RelationalPropertyValueProvider newValueProvider(RowDocumentAccessor documentAccessor,
															   ValueExpressionEvaluator evaluator, ConversionContext context) {

		if (context instanceof ResolvingConversionContext rcc) {

			AggregatePathValueProvider delegate = (AggregatePathValueProvider) super.newValueProvider(documentAccessor,
					evaluator, context);

			return new ResolvingRelationalPropertyValueProvider(delegate, documentAccessor, rcc, rcc.identifier());
		}

		return super.newValueProvider(documentAccessor, evaluator, context);
	}

	/**
	 * {@link RelationalPropertyValueProvider} using a resolving context to lookup relations. This is highly
	 * context-sensitive. Note that the identifier is held here because of a chicken and egg problem, while
	 * {@link ResolvingConversionContext} hols the {@link AggregatePath}.
	 */
	class ResolvingRelationalPropertyValueProvider implements RelationalPropertyValueProvider {

		private final AggregatePathValueProvider delegate;

		private final RowDocumentAccessor accessor;

		private final ResolvingConversionContext context;

		private final Identifier identifier;

		private ResolvingRelationalPropertyValueProvider(AggregatePathValueProvider delegate, RowDocumentAccessor accessor,
														 ResolvingConversionContext context, Identifier identifier) {

			AggregatePath path = context.aggregatePath();

			this.delegate = delegate;
			this.accessor = accessor;
			this.context = context;
			this.identifier = path.isEntity()
					? potentiallyAppendIdentifier(identifier, path.getRequiredLeafEntity(),
					property -> delegate.getValue(path.append(property)))
					: identifier;
		}

		/**
		 * Conditionally append the identifier if the entity has an identifier property.
		 */
		static Identifier potentiallyAppendIdentifier(Identifier base, RelationalPersistentEntity<?> entity,
													  Function<RelationalPersistentProperty, Object> getter) {

			if (entity.hasIdProperty()) {

				RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
				Object propertyValue = getter.apply(idProperty);

				if (propertyValue != null) {
					return base.withPart(idProperty.getColumnName(), propertyValue, idProperty.getType());
				}
			}

			return base;
		}

		@SuppressWarnings("unchecked")
		@Nullable
		@Override
		public <T> T getPropertyValue(RelationalPersistentProperty property) {

			AggregatePath aggregatePath = this.context.aggregatePath();

			if (getConversions().isSimpleType(property.getActualType())) {
				return (T) delegate.getValue(aggregatePath);
			}

			if (property.isEntity()) {

				if (property.isCollectionLike() || property.isMap()) {

					Identifier identifierToUse = this.identifier;
					AggregatePath idDefiningParentPath = aggregatePath.getIdDefiningParentPath();

					// note that the idDefiningParentPath might not itself have an id property, but have a combination of back
					// references and possibly keys, that form an id
					if (idDefiningParentPath.hasIdProperty()) {

						RelationalPersistentProperty identifier = idDefiningParentPath.getRequiredIdProperty();
						AggregatePath idPath = idDefiningParentPath.append(identifier);
						Object value = delegate.getValue(idPath);

						Assert.state(value != null, "Identifier value must not be null at this point");

						identifierToUse = Identifier.of(aggregatePath.getTableInfo().reverseColumnInfo().name(), value,
								identifier.getActualType());
					}

					Iterable<Object> allByPath = relationResolver.findAllByPath(identifierToUse,
							aggregatePath.getRequiredPersistentPropertyPath());

					if (property.isCollectionLike()) {
						return (T) allByPath;
					}

					if (property.isMap()) {
						return (T) ITERABLE_OF_ENTRY_TO_MAP_CONVERTER.convert(allByPath);
					}

					Iterator<Object> iterator = allByPath.iterator();
					if (iterator.hasNext()) {
						return (T) iterator.next();
					}

					return null;
				}

				return hasValue(property) ? (T) readAggregate(this.context, accessor, property.getTypeInformation()) : null;
			}

			return (T) delegate.getValue(aggregatePath);
		}

		@Override
		public boolean hasValue(RelationalPersistentProperty property) {

			if ((property.isCollectionLike() && property.isEntity()) || property.isMap()) {
				// attempt relation fetch
				return true;
			}

			AggregatePath aggregatePath = context.aggregatePath();

			if (property.isEntity()) {

				RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(property);
				if (entity.hasIdProperty()) {

					RelationalPersistentProperty referenceId = entity.getRequiredIdProperty();
					AggregatePath toUse = aggregatePath.append(referenceId);
					return delegate.hasValue(toUse);
				}

				return delegate.hasValue(aggregatePath.getTableInfo().reverseColumnInfo().alias());
			}

			return delegate.hasValue(aggregatePath);
		}

		@Override
		public boolean hasNonEmptyValue(RelationalPersistentProperty property) {

			if ((property.isCollectionLike() && property.isEntity()) || property.isMap()) {
				// attempt relation fetch
				return true;
			}

			AggregatePath aggregatePath = context.aggregatePath();

			if (property.isEntity()) {

				RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(property);
				if (entity.hasIdProperty()) {

					RelationalPersistentProperty referenceId = entity.getRequiredIdProperty();
					AggregatePath toUse = aggregatePath.append(referenceId);
					return delegate.hasValue(toUse);
				}

				return delegate.hasValue(aggregatePath.getTableInfo().reverseColumnInfo().alias());
			}

			return delegate.hasNonEmptyValue(aggregatePath);
		}

		@Override
		public RelationalPropertyValueProvider withContext(ConversionContext context) {

			return context == this.context ? this
					: new ResolvingRelationalPropertyValueProvider(delegate.withContext(context), accessor,
					(ResolvingConversionContext) context, identifier);
		}
	}

	/**
	 * Marker object to indicate that the property value provider should resolve relations.
	 *
	 * @param delegate
	 * @param aggregatePath
	 * @param identifier
	 */
	private record ResolvingConversionContext(ConversionContext delegate, AggregatePath aggregatePath,
											  Identifier identifier) implements ConversionContext {

		@Override
		public <S> S convert(Object source, TypeInformation<? extends S> typeHint) {
			return delegate.convert(source, typeHint);
		}

		@Override
		public <S> S convert(Object source, TypeInformation<? extends S> typeHint, ConversionContext context) {
			return delegate.convert(source, typeHint, context);
		}

		@Override
		public ResolvingConversionContext forProperty(String name) {
			RelationalPersistentProperty property = aggregatePath.getRequiredLeafEntity().getRequiredPersistentProperty(name);
			return forProperty(property);
		}

		@Override
		public ResolvingConversionContext forProperty(RelationalPersistentProperty property) {
			ConversionContext nested = delegate.forProperty(property);
			return new ResolvingConversionContext(nested, aggregatePath.append(property), identifier);
		}

		@Override
		public ResolvingConversionContext withPath(ObjectPath currentPath) {
			return new ResolvingConversionContext(delegate.withPath(currentPath), aggregatePath, identifier);
		}

		@Override
		public ObjectPath getPath() {
			return delegate.getPath();
		}

		@Override
		public CustomConversions getCustomConversions() {
			return delegate.getCustomConversions();
		}

		@Override
		public RelationalConverter getSourceConverter() {
			return delegate.getSourceConverter();
		}
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

}
