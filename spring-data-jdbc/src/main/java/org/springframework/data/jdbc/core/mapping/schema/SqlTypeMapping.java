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
package org.springframework.data.jdbc.core.mapping.schema;

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Strategy interface for mapping a {@link RelationalPersistentProperty} to a Database type.
 *
 * @author Kurt Niemi
 * @author Mark Paluch
 * @author Evgenii Koba
 * @author Jens Schauder
 * @since 3.2
 */
@FunctionalInterface
public interface SqlTypeMapping {

	/**
	 * Determines a column type for a persistent property.
	 *
	 * @param property the property for which the type should be determined.
	 * @return the SQL type to use, such as {@code VARCHAR} or {@code NUMERIC}. Can be {@literal null} if the strategy
	 *         cannot provide a column type.
	 */
	@Nullable
	String getColumnType(RelationalPersistentProperty property);

	/**
	 * Determines a column type for Class.
	 *
	 * @param type class for which the type should be determined.
	 * @return the SQL type to use, such as {@code VARCHAR} or {@code NUMERIC}. Can be {@literal null} if the strategy
	 *         cannot provide a column type.
	 *
	 * @since 3.3
	 */
	@Nullable
	default String getColumnType(Class<?> type) {
		return null;
	}

	/**
	 * Returns the required column type for a persistent property or throws {@link IllegalArgumentException} if the type
	 * cannot be determined.
	 *
	 * @param property the property for which the type should be determined.
	 * @return the SQL type to use, such as {@code VARCHAR} or {@code NUMERIC}. Can be {@literal null} if the strategy
	 *         cannot provide a column type.
	 * @throws IllegalArgumentException if the column type cannot be determined.
	 */
	default String getRequiredColumnType(RelationalPersistentProperty property) {

		String columnType = getColumnType(property);

		if (ObjectUtils.isEmpty(columnType)) {
			throw new IllegalArgumentException(String.format("Cannot determined required column type for %s", property));
		}

		return columnType;
	}

	/**
	 * Determine whether a column is nullable.
	 *
	 * @param property the property for which nullability should be determined.
	 * @return whether the property is nullable.
	 */
	default boolean isNullable(RelationalPersistentProperty property) {
		return !property.getActualType().isPrimitive();
	}

	/**
	 * Returns a composed {@link SqlTypeMapping} that represents a fallback of this type mapping and another. When
	 * evaluating the composed predicate, if this mapping does not contain a column mapping (i.e.
	 * {@link #getColumnType(RelationalPersistentProperty)} returns{@literal null}), then the {@code other} mapping is
	 * evaluated.
	 * <p>
	 * Any exceptions thrown during evaluation of either type mapping are relayed to the caller; if evaluation of this
	 * type mapping throws an exception, the {@code other} predicate will not be evaluated.
	 *
	 * @param other a type mapping that will be used as fallback, must not be {@literal null}.
	 * @return a composed type mapping
	 */
	default SqlTypeMapping and(SqlTypeMapping other) {

		Assert.notNull(other, "Other SqlTypeMapping must not be null");

		return property -> {

			String columnType = getColumnType(property);

			if (ObjectUtils.isEmpty(columnType)) {
				return other.getColumnType(property);
			}

			return columnType;
		};
	}

}
