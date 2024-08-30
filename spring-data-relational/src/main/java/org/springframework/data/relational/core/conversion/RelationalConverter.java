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
package org.springframework.data.relational.core.conversion;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
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
	 * Returns the underlying {@link ConversionService} used by the converter.
	 *
	 * @return never {@literal null}.
	 */
	ConversionService getConversionService();

	/**
	 * Return the underlying {@link EntityInstantiators}.
	 *
	 * @since 2.3
	 */
	EntityInstantiators getEntityInstantiators();

	/**
	 * Returns the underlying {@link MappingContext} used by the converter.
	 *
	 * @return never {@literal null}
	 */
	MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext();

	/**
	 * Return a {@link PersistentPropertyAccessor} to access property values of the {@code instance}.
	 *
	 * @param persistentEntity the kind of entity to operate on. Must not be {@code null}.
	 * @param instance the instance to operate on. Must not be {@code null}.
	 * @return guaranteed to be not {@code null}.
	 */
	<T> PersistentPropertyPathAccessor<T> getPropertyAccessor(PersistentEntity<T, ?> persistentEntity, T instance);

	/**
	 * Introspect the given {@link Class result type} in the context of the {@link Class entity type} whether the returned
	 * type is a projection and what property paths are participating in the projection.
	 *
	 * @param resultType the type to project on. Must not be {@literal null}.
	 * @param entityType the source domain type. Must not be {@literal null}.
	 * @return the introspection result.
	 * @since 3.2
	 * @see EntityProjectionIntrospector#introspect(Class, Class)
	 */
	<M, D> EntityProjection<M, D> introspectProjection(Class<M> resultType, Class<D> entityType);

	/**
	 * Apply a projection to {@link RowDocument} and return the projection return type {@code R}.
	 * {@link EntityProjection#isProjection() Non-projecting} descriptors fall back to {@link #read(Class, RowDocument)
	 * regular object materialization}.
	 *
	 * @param descriptor the projection descriptor, must not be {@literal null}.
	 * @param document must not be {@literal null}.
	 * @return a new instance of the projection return type {@code R}.
	 * @since 3.2
	 */
	<R> R project(EntityProjection<R, ?> descriptor, RowDocument document);

	/**
	 * Read a {@link RowDocument} into the requested {@link Class aggregate type}.
	 *
	 * @param type target aggregate type.
	 * @param source source {@link RowDocument}.
	 * @return the converted object.
	 * @param <R> aggregate type.
	 * @since 3.2
	 */
	<R> R read(Class<R> type, RowDocument source);

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
