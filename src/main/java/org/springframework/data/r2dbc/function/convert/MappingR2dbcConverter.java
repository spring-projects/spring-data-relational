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
package org.springframework.data.r2dbc.function.convert;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.springframework.core.convert.ConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.r2dbc.dialect.ArrayColumns;
import org.springframework.data.r2dbc.domain.OutboundRow;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Converter for R2DBC.
 *
 * @author Mark Paluch
 */
public class MappingR2dbcConverter extends BasicRelationalConverter implements R2dbcConverter {

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		super(context, new R2dbcCustomConversions(Collections.emptyList()));
	}

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions) {
		super(context, conversions);
	}

	// ----------------------------------
	// Entity reading
	// ----------------------------------

	@Override
	public <R> R read(Class<R> type, Row row) {

		TypeInformation<? extends R> typeInfo = ClassTypeInformation.from(type);
		Class<? extends R> rawType = typeInfo.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return type.cast(row);
		}

		if (getConversions().hasCustomReadTarget(Row.class, rawType)
				|| getConversionService().canConvert(Row.class, rawType)) {
			return getConversionService().convert(row, rawType);
		}

		return read(getRequiredPersistentEntity(type), row);
	}

	private <R> R read(RelationalPersistentEntity<R> entity, Row row) {

		R result = createInstance(row, "", entity);

		ConvertingPropertyAccessor<R> propertyAccessor = new ConvertingPropertyAccessor<>(
				entity.getPropertyAccessor(result), getConversionService());

		for (RelationalPersistentProperty property : entity) {

			if (entity.isConstructorArgument(property)) {
				continue;
			}

			propertyAccessor.setProperty(property, readFrom(row, property, ""));
		}

		return result;
	}

	/**
	 * Read a single value or a complete Entity from the {@link Row} passed as an argument.
	 *
	 * @param row the {@link Row} to extract the value from. Must not be {@literal null}.
	 * @param property the {@link RelationalPersistentProperty} for which the value is intended. Must not be
	 *          {@literal null}.
	 * @param prefix to be used for all column names accessed by this method. Must not be {@literal null}.
	 * @return the value read from the {@link Row}. May be {@literal null}.
	 */
	private Object readFrom(Row row, RelationalPersistentProperty property, String prefix) {

		try {

			if (property.isEntity()) {
				return readEntityFrom(row, property);
			}

			Object value = row.get(prefix + property.getColumnName());
			return getPotentiallyConvertedSimpleRead(value, property.getTypeInformation().getType());

		} catch (Exception o_O) {
			throw new MappingException(String.format("Could not read property %s from result set!", property), o_O);
		}
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value
	 * @param target must not be {@literal null}.
	 * @return
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(@Nullable Object value, @Nullable Class<?> target) {

		if (value == null || target == null || ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (getConversions().hasCustomReadTarget(value.getClass(), target)) {
			return getConversionService().convert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return getConversionService().convert(value, target);
	}

	private <S> S readEntityFrom(Row row, PersistentProperty<?> property) {

		String prefix = property.getName() + "_";

		RelationalPersistentEntity<S> entity = (RelationalPersistentEntity<S>) getMappingContext()
				.getRequiredPersistentEntity(property.getActualType());

		if (readFrom(row, entity.getRequiredIdProperty(), prefix) == null) {
			return null;
		}

		S instance = createInstance(row, prefix, entity);

		PersistentPropertyAccessor<S> accessor = entity.getPropertyAccessor(instance);
		ConvertingPropertyAccessor<S> propertyAccessor = new ConvertingPropertyAccessor<>(accessor, getConversionService());

		for (RelationalPersistentProperty p : entity) {
			if (!entity.isConstructorArgument(property)) {
				propertyAccessor.setProperty(p, readFrom(row, p, prefix));
			}
		}

		return instance;
	}

	private <S> S createInstance(Row row, String prefix, RelationalPersistentEntity<S> entity) {

		RowParameterValueProvider rowParameterValueProvider = new RowParameterValueProvider(row, entity, this, prefix);

		return createInstance(entity, rowParameterValueProvider::getParameterValue);
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

		writeProperties(sink, entity, propertyAccessor);
	}

	private void writeProperties(OutboundRow sink, RelationalPersistentEntity<?> entity,
			PersistentPropertyAccessor<?> accessor) {

		for (RelationalPersistentProperty property : entity) {

			if (!property.isWritable()) {
				continue;
			}

			Object value = accessor.getProperty(property);

			if (value == null) {
				writeNullInternal(sink, property);
				continue;
			}

			if (!getConversions().isSimpleType(value.getClass())) {

				RelationalPersistentEntity<?> nestedEntity = getMappingContext().getPersistentEntity(property.getActualType());
				if (nestedEntity != null) {
					throw new InvalidDataAccessApiUsageException("Nested entities are not supported");
				}
			}

			writeSimpleInternal(sink, value, property);
		}
	}

	private void writeSimpleInternal(OutboundRow sink, Object value, RelationalPersistentProperty property) {
		sink.put(property.getColumnName(), SettableValue.from(getPotentiallyConvertedSimpleWrite(value)));
	}

	private void writeNullInternal(OutboundRow sink, RelationalPersistentProperty property) {

		sink.put(property.getColumnName(),
				SettableValue.empty(getPotentiallyConvertedSimpleNullType(property.getType())));
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
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple Mongo type.
	 * Returns the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 *
	 * @param value
	 * @return
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleWrite(@Nullable Object value) {

		if (value == null) {
			return null;
		}

		Optional<Class<?>> customTarget = getConversions().getCustomWriteTarget(value.getClass());

		if (customTarget.isPresent()) {
			return getConversionService().convert(value, customTarget.get());
		}

		return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
	}

	public Object getArrayValue(ArrayColumns arrayColumns, RelationalPersistentProperty property, Object value) {

		Class<?> targetType = arrayColumns.getArrayType(property.getActualType());

		if (!property.isArray() || !property.getActualType().equals(targetType)) {

			Object zeroLengthArray = Array.newInstance(targetType, 0);
			return getConversionService().convert(value, zeroLengthArray.getClass());
		}

		return value;
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
	@SuppressWarnings("unchecked")
	public <T> BiFunction<Row, RowMetadata, T> populateIdIfNecessary(T object) {

		Assert.notNull(object, "Entity object must not be null!");

		Class<?> userClass = ClassUtils.getUserClass(object);
		RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(userClass);

		return (row, metadata) -> {

			PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);
			RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();

			if (propertyAccessor.getProperty(idProperty) == null) {

				if (potentiallySetId(row, metadata, propertyAccessor, idProperty)) {
					return (T) propertyAccessor.getBean();
				}
			}

			return object;
		};
	}

	private boolean potentiallySetId(Row row, RowMetadata metadata, PersistentPropertyAccessor<?> propertyAccessor,
			RelationalPersistentProperty idProperty) {

		Map<String, ColumnMetadata> columns = createMetadataMap(metadata);
		Object generatedIdValue = null;

		if (columns.containsKey(idProperty.getColumnName())) {
			generatedIdValue = row.get(idProperty.getColumnName());
		}

		if (columns.size() == 1) {

			String key = columns.keySet().iterator().next();
			generatedIdValue = row.get(key);
		}

		if (generatedIdValue != null) {

			ConversionService conversionService = getConversionService();
			propertyAccessor.setProperty(idProperty, conversionService.convert(generatedIdValue, idProperty.getType()));
			return true;
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	private <R> RelationalPersistentEntity<R> getRequiredPersistentEntity(Class<R> type) {
		return (RelationalPersistentEntity) getMappingContext().getRequiredPersistentEntity(type);
	}

	private static Map<String, ColumnMetadata> createMetadataMap(RowMetadata metadata) {

		Map<String, ColumnMetadata> columns = new LinkedHashMap<>();

		for (ColumnMetadata column : metadata.getColumnMetadatas()) {
			columns.put(column.getName(), column);
		}

		return columns;
	}

	@RequiredArgsConstructor
	private static class RowParameterValueProvider implements ParameterValueProvider<RelationalPersistentProperty> {

		private final @NonNull Row resultSet;
		private final @NonNull RelationalPersistentEntity<?> entity;
		private final @NonNull RelationalConverter converter;
		private final @NonNull String prefix;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		@Nullable
		public <T> T getParameterValue(Parameter<T, RelationalPersistentProperty> parameter) {

			RelationalPersistentProperty property = entity.getRequiredPersistentProperty(parameter.getName());
			String column = prefix + property.getColumnName();

			try {
				return converter.getConversionService().convert(resultSet.get(column), parameter.getType().getType());
			} catch (Exception o_O) {
				throw new MappingException(String.format("Couldn't read column %s from Row.", column), o_O);
			}
		}
	}
}
