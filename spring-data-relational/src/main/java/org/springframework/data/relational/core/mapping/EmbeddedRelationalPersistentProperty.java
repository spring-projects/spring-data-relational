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
package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.springframework.data.mapping.Association;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Embedded property extension to a {@link RelationalPersistentProperty}
 *
 * @author Mark Paluch
 * @since 3.2
 */
class EmbeddedRelationalPersistentProperty implements RelationalPersistentProperty {

	private final RelationalPersistentProperty delegate;

	private final EmbeddedContext context;

	public EmbeddedRelationalPersistentProperty(RelationalPersistentProperty delegate, EmbeddedContext context) {

		this.delegate = delegate;
		this.context = context;
	}

	@Override
	public boolean isEmbedded() {
		return delegate.isEmbedded();
	}

	@Nullable
	@Override
	public String getEmbeddedPrefix() {
		return context.withEmbeddedPrefix(delegate.getEmbeddedPrefix());
	}

	@Override
	public SqlIdentifier getColumnName() {
		return delegate.getColumnName().transform(context::withEmbeddedPrefix);
	}

	@Override
	public boolean hasExplicitColumnName() {
		return delegate.hasExplicitColumnName();
	}

	@Override
	public RelationalPersistentEntity<?> getOwner() {
		return delegate.getOwner();
	}

	@Override
	public SqlIdentifier getReverseColumnName(RelationalPersistentEntity<?> owner) {
		return delegate.getReverseColumnName(owner);
	}

	@Override
	@Nullable
	public SqlIdentifier getKeyColumn() {
		return delegate.getKeyColumn();
	}

	@Override
	public boolean isQualified() {
		return delegate.isQualified();
	}

	@Override
	public Class<?> getQualifierColumnType() {
		return delegate.getQualifierColumnType();
	}

	@Override
	public boolean isOrdered() {
		return delegate.isOrdered();
	}

	@Override
	public boolean shouldCreateEmptyEmbedded() {
		return delegate.shouldCreateEmptyEmbedded();
	}

	@Override
	public boolean isInsertOnly() {
		return delegate.isInsertOnly();
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public Class<?> getType() {
		return delegate.getType();
	}

	@Override
	public TypeInformation<?> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypeInformation() {
		return delegate.getPersistentEntityTypeInformation();
	}

	@Override
	@Nullable
	public Method getGetter() {
		return delegate.getGetter();
	}

	@Override
	@Nullable
	public Method getSetter() {
		return delegate.getSetter();
	}

	@Override
	@Nullable
	public Method getWither() {
		return delegate.getWither();
	}

	@Override
	@Nullable
	public Field getField() {
		return delegate.getField();
	}

	@Override
	@Nullable
	public String getSpelExpression() {
		return delegate.getSpelExpression();
	}

	@Override
	@Nullable
	public Association<RelationalPersistentProperty> getAssociation() {
		return delegate.getAssociation();
	}

	@Override
	public Association<RelationalPersistentProperty> getRequiredAssociation() {
		return delegate.getRequiredAssociation();
	}

	@Override
	public boolean isEntity() {
		return delegate.isEntity();
	}

	@Override
	public boolean isIdProperty() {
		return delegate.isIdProperty();
	}

	@Override
	public boolean isVersionProperty() {
		return delegate.isVersionProperty();
	}

	@Override
	public boolean isCollectionLike() {
		return delegate.isCollectionLike();
	}

	@Override
	public boolean isMap() {
		return delegate.isMap();
	}

	@Override
	public boolean isArray() {
		return delegate.isArray();
	}

	@Override
	public boolean isTransient() {
		return delegate.isTransient();
	}

	@Override
	public boolean isWritable() {
		return delegate.isWritable();
	}

	@Override
	public boolean isReadable() {
		return delegate.isReadable();
	}

	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean isAssociation() {
		return delegate.isAssociation();
	}

	@Override
	@Nullable
	public Class<?> getComponentType() {
		return delegate.getComponentType();
	}

	@Override
	public Class<?> getRawType() {
		return delegate.getRawType();
	}

	@Override
	@Nullable
	public Class<?> getMapValueType() {
		return delegate.getMapValueType();
	}

	@Override
	public Class<?> getActualType() {
		return delegate.getActualType();
	}

	@Override
	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	@Override
	@Nullable
	public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
		return delegate.findPropertyOrOwnerAnnotation(annotationType);
	}

	@Override
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	@Override
	public boolean usePropertyAccess() {
		return delegate.usePropertyAccess();
	}

	@Override
	public boolean hasActualTypeAnnotation(Class<? extends Annotation> annotationType) {
		return delegate.hasActualTypeAnnotation(annotationType);
	}

	@Override
	@Nullable
	public Class<?> getAssociationTargetType() {
		return delegate.getAssociationTargetType();
	}

	@Override
	@Nullable
	public TypeInformation<?> getAssociationTargetTypeInformation() {
		return delegate.getAssociationTargetTypeInformation();
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (delegate == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		EmbeddedRelationalPersistentProperty that = (EmbeddedRelationalPersistentProperty) o;

		if (!ObjectUtils.nullSafeEquals(delegate, that.delegate)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(context, that.context);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(delegate);
		result = 31 * result + ObjectUtils.nullSafeHashCode(context);
		return result;
	}

	@Override
	public String toString() {
		return delegate.toString();
	}
}
