/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Utilities commonly used to set/get properties for instances of RelationalPersistentEntities.
 *
 * @author Tyler Van Gorder
 * @since 2.0
 */
public class RelationalEntityVersionUtils {

	private RelationalEntityVersionUtils() {}

	/**
	 * Get the current value of the version property for an instance of a relational persistent entity.
	 *
	 * @param instance must not be {@literal null}.
	 * @param persistentEntity must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @return Current value of the version property
	 * @throws IllegalArgumentException if the entity does not have a version property.
	 */
	@Nullable
	public static <S> Number getVersionNumberFromEntity(S instance, RelationalPersistentEntity<S> persistentEntity,
			RelationalConverter converter) {

		if (!persistentEntity.hasVersionProperty()) {
			throw new IllegalArgumentException("The entity does not have a version property.");
		}

		ConvertingPropertyAccessor<S> convertingPropertyAccessor = new ConvertingPropertyAccessor<>(
				persistentEntity.getPropertyAccessor(instance), converter.getConversionService());
		return convertingPropertyAccessor.getProperty(persistentEntity.getRequiredVersionProperty(), Number.class);
	}

	/**
	 * Set the version property on an instance of a relational persistent entity. This method returns an instance of the
	 * same type with the updated version property and will correctly handle the case where the version is immutable.
	 *
	 * @param instance must not be {@literal null}.
	 * @param version The value to be set on the version property.
	 * @param persistentEntity must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @return An instance of the entity with an updated version property.
	 * @throws IllegalArgumentException if the entity does not have a version property.
	 */
	public static <S> S setVersionNumberOnEntity(S instance, @Nullable Number version,
			RelationalPersistentEntity<S> persistentEntity, RelationalConverter converter) {

		if (!persistentEntity.hasVersionProperty()) {
			throw new IllegalArgumentException("The entity does not have a version property.");
		}

		PersistentPropertyAccessor<S> propertyAccessor = converter.getPropertyAccessor(persistentEntity, instance);
		RelationalPersistentProperty versionProperty = persistentEntity.getRequiredVersionProperty();
		propertyAccessor.setProperty(versionProperty, version);

		return propertyAccessor.getBean();
	}
}
