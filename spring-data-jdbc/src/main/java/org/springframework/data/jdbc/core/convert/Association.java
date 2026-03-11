/*
 * Copyright 2026-present the original author or authors.
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

import java.util.Iterator;

import org.jspecify.annotations.Nullable;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * Value class to hold association information.
 *
 * @author Mark Paluch
 * @since 4.0.4
 */
class Association {

	private final @Nullable RelationalPersistentEntity<?> identifierEntity;

	private Association(
			@Nullable RelationalPersistentEntity<?> identifierEntity) {
		this.identifierEntity = identifierEntity;
	}

	/**
	 * Determine whether the given property is an association.
	 */
	public static boolean isAssociation(RelationalPersistentProperty property) {
		return property.isAssociation() && !property.isQualified();
	}

	/**
	 * Detect an {@code Association} for the given property if it is an association, otherwise return {@code null}.
	 */
	public static @Nullable Association detect(RelationalPersistentProperty property, JdbcConverter converter) {
		return isAssociation(property) ? from(property, converter) : null;
	}

	/**
	 * Introspect the property and create an {@code Association} instance.
	 */
	public static Association from(RelationalPersistentProperty property, JdbcConverter converter) {

		RelationalMappingContext context = converter.getMappingContext();

		TypeInformation<?> targetType = property.getAssociationTargetTypeInformation();
		TypeInformation<?> idType = null;
		if (AggregateReference.class.isAssignableFrom(property.getType())) {
			idType = property.getTypeInformation().getRequiredMapValueType();
		} else if (targetType != null) {
			RelationalPersistentEntity<?> targetEntity = context.getPersistentEntity(targetType);
			if (targetEntity != null) {
				idType = targetEntity.getRequiredIdProperty().getTypeInformation();
			}
		} else {
			throw new IllegalArgumentException("Cannot determine reference type type for " + property);
		}

		if (idType == null) {
			throw new IllegalArgumentException("Cannot determine reference identifier type for " + property);
		}

		RelationalPersistentEntity<?> identifierEntity = context.getPersistentEntity(idType);

		if (identifierEntity == null || !hasMultipleColumns(identifierEntity)
				|| (converter instanceof MappingJdbcConverter mc
						&& mc.getConversions().hasCustomWriteTarget(idType.getType()))) {
			return new Association(null);
		}

		return new Association(context.getPersistentEntity(idType));
	}

	private static boolean hasMultipleColumns(@Nullable RelationalPersistentEntity<?> identifierEntity) {
		Iterator<RelationalPersistentProperty> iterator = identifierEntity.iterator();
		if (iterator.hasNext()) {
			iterator.next();
			return iterator.hasNext();
		}
		return false;
	}

	/**
	 * Return whether the identifier is a complex type with more than one column. Simple identifier types are either one
	 * of the following list:
	 * <ul>
	 * <li>A simple type</li>
	 * <li>A type for which a write-converter is registered</li>
	 * <li>Consisting of a single property</li>
	 * </ul>
	 */
	public boolean isComplexIdentifier() {
		return identifierEntity != null;
	}

	public RelationalPersistentEntity<?> getRequiredTargetIdentifierEntity() {

		Assert.state(identifierEntity != null, "Target identifier is not a persistent entity");
		return identifierEntity;
	}
}
