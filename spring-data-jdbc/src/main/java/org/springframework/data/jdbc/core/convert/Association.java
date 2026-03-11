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
 * @since 4.0.3
 */
class Association {

	private final RelationalPersistentEntity<?> targetType;

	private final TypeInformation<?> identifierType;

	private final @Nullable RelationalPersistentEntity<?> identifierEntity;

	private Association(RelationalPersistentEntity<?> targetType, TypeInformation<?> identifierType,
			@Nullable RelationalPersistentEntity<?> identifierEntity) {
		this.targetType = targetType;
		this.identifierType = identifierType;
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
		TypeInformation<?> idType;
		if (AggregateReference.class.isAssignableFrom(property.getType())) {
			idType = property.getTypeInformation().getRequiredMapValueType();
		} else if (targetType != null) {
			idType = context.getPersistentEntity(targetType).getRequiredIdProperty().getTypeInformation();
		} else {
			throw new IllegalArgumentException("Cannot determine reference type type for " + property);
		}

		RelationalPersistentEntity<?> targetEntity = context.getRequiredPersistentEntity(targetType);
		RelationalPersistentEntity<?> identifierEntity = context.getPersistentEntity(idType);

		if (identifierEntity == null || !hasMultipleColumns(identifierEntity)
				|| (converter instanceof MappingJdbcConverter mc
						&& mc.getConversions().hasCustomWriteTarget(idType.getType()))) {
			return new Association(targetEntity, idType, null);
		}

		return new Association(targetEntity, idType, context.getPersistentEntity(idType));
	}

	private static boolean hasMultipleColumns(@Nullable RelationalPersistentEntity<?> identifierEntity) {
		Iterator<RelationalPersistentProperty> iterator = identifierEntity.iterator();
		boolean multiColumn = iterator.hasNext() && iterator.hasNext();
		return multiColumn;
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
