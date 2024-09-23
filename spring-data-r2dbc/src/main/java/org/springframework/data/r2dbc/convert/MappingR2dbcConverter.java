/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.convert;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.springframework.core.convert.ConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.support.ArrayUtils;
import org.springframework.data.relational.core.conversion.MappingRelationalConverter;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * Converter for R2DBC.
 *
 * @author Mark Paluch
 * @author Oliver Drotbohm
 */
public class MappingR2dbcConverter extends MappingRelationalConverter implements R2dbcConverter {

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		super((RelationalMappingContext) context,
				new R2dbcCustomConversions(R2dbcCustomConversions.STORE_CONVERSIONS, Collections.emptyList()));
	}

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions) {
		super((RelationalMappingContext) context, conversions);
	}

	// ----------------------------------
	// Entity reading
	// ----------------------------------

	@Override
	public <R> R read(Class<R> type, Row row) {
		return read(type, row, null);
	}

	@Override
	public <R> R read(Class<R> type, Row row, @Nullable RowMetadata metadata) {

		TypeInformation<? extends R> typeInfo = TypeInformation.of(type);
		Class<? extends R> rawType = typeInfo.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return type.cast(row);
		}

		if (getConversions().hasCustomReadTarget(Row.class, rawType)
				&& getConversionService().canConvert(Row.class, rawType)) {
			return getConversionService().convert(row, rawType);
		}

		RowDocument document = toRowDocument(type, row, metadata != null ? metadata.getColumnMetadatas() : null);
		return read(type, document);
	}

	@Override
	public RowDocument toRowDocument(Class<?> type, Readable row,
			@Nullable Iterable<? extends ReadableMetadata> metadata) {

		RowDocument document = new RowDocument();
		RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(type);

		if (persistentEntity != null) {
			captureRowValues(row, metadata, document, persistentEntity);
		}

		if (metadata != null) {
			for (ReadableMetadata m : metadata) {

				if (document.containsKey(m.getName())) {
					continue;
				}

				document.put(m.getName(), row.get(m.getName()));
			}
		}

		return document;
	}

	private static void captureRowValues(Readable row, @Nullable Iterable<? extends ReadableMetadata> metadata,
			RowDocument document, RelationalPersistentEntity<?> persistentEntity) {

		for (RelationalPersistentProperty property : persistentEntity) {

			String identifier = property.getColumnName().getReference();

			if (property.isEntity() || (metadata != null && !RowMetadataUtils.containsColumn(metadata, identifier))) {
				continue;
			}

			Object value;
			Class<?> propertyType = property.getType();

			if (propertyType.equals(Clob.class)) {
				value = row.get(identifier, Clob.class);
			} else if (propertyType.equals(Blob.class)) {
				value = row.get(identifier, Blob.class);
			} else {
				value = row.get(identifier);
			}

			document.put(identifier, value);
		}
	}

	// ----------------------------------
	// Entity writing
	// ----------------------------------

	@Override
	public void write(Object source, OutboundRow sink) {

		Class<?> userClass = ClassUtils.getUserClass(source);

		Optional<Class<?>> customTarget = getConversions().getCustomWriteTarget(userClass, OutboundRow.class);
		if (customTarget.isPresent()) {

			OutboundRow result = getConversionService().convert(source, OutboundRow.class);
			sink.putAll(result);
			return;
		}

		writeInternal(source, sink, userClass);
	}

	private void writeInternal(Object source, OutboundRow sink, Class<?> userClass) {

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(userClass);
		PersistentPropertyAccessor<?> propertyAccessor = entity.getPropertyAccessor(source);

		writeProperties(sink, entity, propertyAccessor, entity.isNew(source));
	}

	private void writeProperties(OutboundRow sink, RelationalPersistentEntity<?> entity,
			PersistentPropertyAccessor<?> accessor, boolean isNew) {

		for (RelationalPersistentProperty property : entity) {

			if (!property.isWritable()) {
				continue;
			}

			Object value;

			if (property.isIdProperty()) {
				IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(accessor.getBean());
				value = identifierAccessor.getIdentifier();
			} else {
				value = accessor.getProperty(property);
			}

			if (value == null) {
				writeNullInternal(sink, property);
				continue;
			}

			if (getConversions().isSimpleType(value.getClass())) {
				writeSimpleInternal(sink, value, isNew, property);
			} else {
				writePropertyInternal(sink, value, isNew, property);
			}
		}
	}

	private void writeSimpleInternal(OutboundRow sink, Object value, boolean isNew,
			RelationalPersistentProperty property) {

		Object result = getPotentiallyConvertedSimpleWrite(value);

		sink.put(property.getColumnName(),
				Parameter.fromOrEmpty(result, getPotentiallyConvertedSimpleNullType(property.getType())));
	}

	private void writePropertyInternal(OutboundRow sink, Object value, boolean isNew,
			RelationalPersistentProperty property) {

		TypeInformation<?> valueType = TypeInformation.of(value.getClass());

		if (valueType.isCollectionLike()) {

			if (valueType.getActualType() != null && valueType.getRequiredActualType().isCollectionLike()) {

				// pass-thru nested collections
				writeSimpleInternal(sink, value, isNew, property);
				return;
			}

			List<Object> collectionInternal = createCollection(asCollection(value), property);
			sink.put(property.getColumnName(), Parameter.from(collectionInternal));
			return;
		}

		throw new InvalidDataAccessApiUsageException("Nested entities are not supported");
	}

	/**
	 * Writes the given {@link Collection} using the given {@link RelationalPersistentProperty} information.
	 *
	 * @param collection must not be {@literal null}.
	 * @param property must not be {@literal null}.
	 * @return
	 */
	protected List<Object> createCollection(Collection<?> collection, RelationalPersistentProperty property) {
		return writeCollectionInternal(collection, property.getTypeInformation(), new ArrayList<>());
	}

	/**
	 * Populates the given {@link Collection sink} with converted values from the given {@link Collection source}.
	 *
	 * @param source the collection to create a {@link Collection} for, must not be {@literal null}.
	 * @param type the {@link TypeInformation} to consider or {@literal null} if unknown.
	 * @param sink the {@link Collection} to write to.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<Object> writeCollectionInternal(Collection<?> source, @Nullable TypeInformation<?> type,
			Collection<?> sink) {

		TypeInformation<?> componentType = null;

		List<Object> collection = sink instanceof List ? (List<Object>) sink : new ArrayList<>(sink);

		if (type != null) {
			componentType = type.getComponentType();
		}

		for (Object element : source) {

			Class<?> elementType = element == null ? null : element.getClass();

			if (elementType == null || getConversions().isSimpleType(elementType)) {
				collection.add(getPotentiallyConvertedSimpleWrite(element,
						componentType != null ? componentType.getType() : Object.class));
			} else if (element instanceof Collection || elementType.isArray()) {
				collection.add(writeCollectionInternal(asCollection(element), componentType, new ArrayList<>()));
			} else {
				throw new InvalidDataAccessApiUsageException("Nested entities are not supported");
			}
		}

		return collection;
	}

	private void writeNullInternal(OutboundRow sink, RelationalPersistentProperty property) {

		sink.put(property.getColumnName(), Parameter.empty(getPotentiallyConvertedSimpleNullType(property.getType())));
	}

	private Class<?> getPotentiallyConvertedSimpleNullType(Class<?> type) {

		Optional<Class<?>> customTarget = getConversions().getCustomWriteTarget(type);

		if (customTarget.isPresent()) {
			return customTarget.get();

		}

		if (type.isEnum()) {
			return String.class;
		}

		return type;
	}

	/**
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple type. Returns
	 * the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 *
	 * @param value
	 * @return
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleWrite(@Nullable Object value) {
		return getPotentiallyConvertedSimpleWrite(value, Object.class);
	}

	/**
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple type. Returns
	 * the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 *
	 * @param value
	 * @return
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleWrite(@Nullable Object value, Class<?> typeHint) {

		if (value == null) {
			return null;
		}

		if (Object.class != typeHint) {

			if (getConversionService().canConvert(value.getClass(), typeHint)) {
				value = getConversionService().convert(value, typeHint);
			}
		}

		Optional<Class<?>> customTarget = getConversions().getCustomWriteTarget(value.getClass());

		if (customTarget.isPresent()) {
			return getConversionService().convert(value, customTarget.get());
		}

		return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
	}

	@Override
	public Object getArrayValue(ArrayColumns arrayColumns, RelationalPersistentProperty property, Object value) {

		Class<?> actualType = null;
		if (value instanceof Collection) {
			actualType = CollectionUtils.findCommonElementType((Collection<?>) value);
		} else if (value.getClass().isArray()) {
			actualType = value.getClass().getComponentType();
		}

		if (actualType == null) {
			actualType = property.getActualType();
		}

		actualType = getTargetType(actualType);

		Class<?> targetType = arrayColumns.getArrayType(actualType);

		if (!property.isArray() || !targetType.isAssignableFrom(value.getClass())) {

			int depth = value.getClass().isArray() ? ArrayUtils.getDimensionDepth(value.getClass()) : 1;
			Class<?> targetArrayType = ArrayUtils.getArrayClass(targetType, depth);
			return getConversionService().convert(value, targetArrayType);
		}

		return value;
	}

	@Override
	public Class<?> getTargetType(Class<?> valueType) {

		Optional<Class<?>> writeTarget = getConversions().getCustomWriteTarget(valueType);

		return writeTarget.orElseGet(() -> {
			return Enum.class.isAssignableFrom(valueType) ? String.class : valueType;
		});
	}

	@Override
	public boolean isSimpleType(Class<?> type) {
		return getConversions().isSimpleType(type);
	}

	// ----------------------------------
	// Id handling
	// ----------------------------------

	/**
	 * Returns a {@link java.util.function.Function} that populates the id property of the {@code object} from a
	 * {@link Row}.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> BiFunction<Row, RowMetadata, T> populateIdIfNecessary(T object) {

		Assert.notNull(object, "Entity object must not be null");

		Class<?> userClass = ClassUtils.getUserClass(object);
		RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(userClass);

		if (!entity.hasIdProperty()) {
			return (row, rowMetadata) -> object;
		}

		return (row, metadata) -> {

			if (metadata == null) {
				metadata = row.getMetadata();
			}

			PersistentPropertyAccessor<?> propertyAccessor = entity.getPropertyAccessor(object);
			RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();

			boolean idPropertyUpdateNeeded = false;

			Object id = propertyAccessor.getProperty(idProperty);
			if (idProperty.getType().isPrimitive()) {
				idPropertyUpdateNeeded = id instanceof Number number && number.longValue() == 0;
			} else {
				idPropertyUpdateNeeded = id == null;
			}

			if (idPropertyUpdateNeeded) {
				return potentiallySetId(row, metadata, propertyAccessor, idProperty) //
						? (T) propertyAccessor.getBean() //
						: object;
			}

			return object;
		};
	}

	private boolean potentiallySetId(Row row, RowMetadata metadata, PersistentPropertyAccessor<?> propertyAccessor,
			RelationalPersistentProperty idProperty) {

		String idColumnName = idProperty.getColumnName().getReference();
		Object generatedIdValue = extractGeneratedIdentifier(row, metadata, idColumnName);

		if (generatedIdValue == null) {
			return false;
		}

		ConversionService conversionService = getConversionService();
		propertyAccessor.setProperty(idProperty, conversionService.convert(generatedIdValue, idProperty.getType()));

		return true;
	}

	@Nullable
	private Object extractGeneratedIdentifier(Row row, RowMetadata metadata, String idColumnName) {

		if (RowMetadataUtils.containsColumn(metadata, idColumnName)) {
			return row.get(idColumnName);
		}

		Iterable<? extends ColumnMetadata> columns = RowMetadataUtils.getColumnMetadata(metadata);
		Iterator<? extends ColumnMetadata> it = columns.iterator();

		if (it.hasNext()) {
			ColumnMetadata column = it.next();
			return row.get(column.getName());
		}

		return null;
	}

	private <R> RelationalPersistentEntity<R> getRequiredPersistentEntity(Class<R> type) {
		return (RelationalPersistentEntity<R>) getMappingContext().getRequiredPersistentEntity(type);
	}

	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 *
	 * @param source
	 * @return
	 */
	private static Collection<?> asCollection(Object source) {

		if (source instanceof Collection) {
			return (Collection<?>) source;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

}
