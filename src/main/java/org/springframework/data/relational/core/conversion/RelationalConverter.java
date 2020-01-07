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
package org.springframework.data.relational.core.conversion;

import java.util.function.Function;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * A {@link RelationalConverter} is responsible for converting for values to the native relational representation and
 * vice versa.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public interface RelationalConverter {

	/**
	 * Returns the underlying {@link MappingContext} used by the converter.
	 *
	 * @return never {@literal null}
	 */
	MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext();

	/**
	 * Returns the underlying {@link ConversionService} used by the converter.
	 *
	 * @return never {@literal null}.
	 */
	ConversionService getConversionService();

	/**
	 * Create a new instance of {@link PersistentEntity} given {@link ParameterValueProvider} to obtain constructor
	 * properties.
	 *
	 * @param entity the kind of entity to create. Must not be {@code null}.
	 * @param parameterValueProvider a function that provides the value to pass to a constructor, given a {@link Parameter}. Must not be {@code null}.
	 * @param <T> the type of entity to create.
	 * @return the instantiated entity. Guaranteed to be not {@code null}.
	 */
	<T> T createInstance(PersistentEntity<T, RelationalPersistentProperty> entity,
			Function<Parameter<?, RelationalPersistentProperty>, Object> parameterValueProvider);

	/**
	 * Return a {@link PersistentPropertyAccessor} to access property values of the {@code instance}.
	 *
	 * @param persistentEntity the kind of entity to operate on. Must not be {@code null}.
	 * @param instance the instance to operate on. Must not be {@code null}.
	 * @return Guaranteed to be not {@code null}.
	 */
	<T> PersistentPropertyAccessor<T> getPropertyAccessor(PersistentEntity<T, ?> persistentEntity, T instance);

	/**
	 * Read a relational value into the desired {@link TypeInformation destination type}.
	 *
	 * @param value a value as it is returned by the driver accessing the persistence store. May be {@code null}.
	 * @param type {@link TypeInformation} into which the value is to be converted. Must not be {@code null}.
	 * @return The converted value. May be {@code null}.
	 */
	@Nullable
	Object readValue(@Nullable Object value, TypeInformation<?> type);

	/**
	 * Write a property value into a relational type that can be stored natively.
	 *
	 * @param value a value as it is used in the object model. May be {@code null}.
	 * @param type {@link TypeInformation} into which the value is to be converted. Must not be {@code null}.
	 * @return The converted value. May be {@code null}.
	 */
	@Nullable
	Object writeValue(@Nullable Object value, TypeInformation<?> type);
}
