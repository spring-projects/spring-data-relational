/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Converter for R2DBC.
 *
 * @author Mark Paluch
 */
public class MappingR2dbcConverter {

	private final RelationalConverter relationalConverter;

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		this(new BasicRelationalConverter(context));
	}

	/**
	 * Creates a new {@link MappingR2dbcConverter} given {@link RelationalConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public MappingR2dbcConverter(RelationalConverter converter) {

		Assert.notNull(converter, "RelationalConverter must not be null!");

		this.relationalConverter = converter;
	}

	/**
	 * Returns a {@link Map} that maps column names to an {@link Optional} value. Used {@link Optional#empty()} if the
	 * underlying property is {@literal null}.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	public Map<String, SettableValue> getColumnsToUpdate(Object object) {

		Assert.notNull(object, "Entity object must not be null!");

		Class<?> userClass = ClassUtils.getUserClass(object);
		RelationalPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(userClass);

		Map<String, SettableValue> update = new LinkedHashMap<>();

		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		for (RelationalPersistentProperty property : entity) {
			update.put(property.getColumnName(),
					new SettableValue(property.getColumnName(), propertyAccessor.getProperty(property), property.getType()));
		}

		return update;
	}

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

			ConversionService conversionService = relationalConverter.getConversionService();
			propertyAccessor.setProperty(idProperty, conversionService.convert(generatedIdValue, idProperty.getType()));
			return true;
		}

		return false;
	}

	private static Map<String, ColumnMetadata> createMetadataMap(RowMetadata metadata) {

		Map<String, ColumnMetadata> columns = new LinkedHashMap<>();

		for (ColumnMetadata column : metadata.getColumnMetadatas()) {
			columns.put(column.getName(), column);
		}

		return columns;
	}

	public MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return relationalConverter.getMappingContext();
	}
}
