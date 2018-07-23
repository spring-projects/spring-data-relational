/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Meta data about a property to be used by repository implementations.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
class BasicRelationalPersistentProperty extends AnnotationBasedPersistentProperty<RelationalPersistentProperty>
		implements RelationalPersistentProperty {

	private static final Map<Class<?>, Class<?>> javaToDbType = new LinkedHashMap<>();

	static {
		javaToDbType.put(Enum.class, String.class);
		javaToDbType.put(ZonedDateTime.class, String.class);
		javaToDbType.put(Temporal.class, Date.class);
	}

	private final RelationalMappingContext context;
	private final Lazy<Optional<String>> columnName;

	/**
	 * Creates a new {@link AnnotationBasedPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param context must not be {@literal null}
	 */
	public BasicRelationalPersistentProperty(Property property, PersistentEntity<?, RelationalPersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder, RelationalMappingContext context) {

		super(property, owner, simpleTypeHolder);

		Assert.notNull(context, "context must not be null.");

		this.context = context;
		this.columnName = Lazy.of(() -> Optional.ofNullable(findAnnotation(Column.class)).map(Column::value));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<RelationalPersistentProperty> createAssociation() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.mapping.model.JdbcPersistentProperty#getColumnName()
	 */
	@Override
	public String getColumnName() {
		return columnName.get().orElseGet(() -> context.getNamingStrategy().getColumnName(this));
	}

	/**
	 * The type to be used to store this property in the database.
	 *
	 * @return a {@link Class} that is suitable for usage with JDBC drivers
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Class getColumnType() {

		Class columnType = columnTypeIfEntity(getActualType());

		return columnType == null ? columnTypeForNonEntity(getActualType()) : columnType;
	}

	@Override
	public RelationalPersistentEntity<?> getOwner() {
		return (RelationalPersistentEntity<?>) super.getOwner();
	}

	@Override
	public String getReverseColumnName() {
		return context.getNamingStrategy().getReverseColumnName(this);
	}

	@Override
	public String getKeyColumn() {
		return isQualified() ? context.getNamingStrategy().getKeyColumn(this) : null;
	}

	@Override
	public boolean isQualified() {
		return isMap() || isListLike();
	}

	@Override
	public boolean isOrdered() {
		return isListLike();
	}

	private boolean isListLike() {
		return isCollectionLike() && !Set.class.isAssignableFrom(this.getType());
	}

	@Nullable
	private Class columnTypeIfEntity(Class type) {

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(type);

		if (persistentEntity == null) {
			return null;
		}

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();

		if (idProperty == null) {
			return null;
		}
		return idProperty.getColumnType();
	}

	private Class columnTypeForNonEntity(Class type) {

		return javaToDbType.entrySet().stream() //
				.filter(e -> e.getKey().isAssignableFrom(type)) //
				.map(e -> (Class) e.getValue()) //
				.findFirst() //
				.orElseGet(() -> ClassUtils.resolvePrimitiveIfNecessary(type));
	}
}
