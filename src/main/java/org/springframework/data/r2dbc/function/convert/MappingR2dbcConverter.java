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

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
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

	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	public MappingR2dbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	/**
	 * Returns a {@link Map} that maps column names to an {@link Optional} value. Used {@link Optional#empty()} if the
	 * underlying property is {@literal null}.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	public Map<String, Optional<Object>> getFieldsToUpdate(Object object) {

		Assert.notNull(object, "Entity object must not be null!");

		Class<?> userClass = ClassUtils.getUserClass(object);
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(userClass);

		Map<String, Optional<Object>> update = new LinkedHashMap<>();

		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		for (RelationalPersistentProperty property : entity) {
			update.put(property.getColumnName(), Optional.ofNullable(propertyAccessor.getProperty(property)));
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
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(userClass);

		return (row, metadata) -> {

			PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);
			RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();

			if (propertyAccessor.getProperty(idProperty) == null) {

				propertyAccessor.setProperty(idProperty, row.get(idProperty.getColumnName(), idProperty.getColumnType()));
				return (T) propertyAccessor.getBean();
			}

			return object;
		};
	}

	public MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> getMappingContext() {
		return mappingContext;
	}
}
