/*
 * Copyright 2017-2019 the original author or authors.
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

import java.lang.reflect.Array;
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
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Meta data about a property to be used by repository implementations.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Florian Lüdiger
 * @author Bastian Wilhelm
 */
public class BasicRelationalPersistentProperty extends AnnotationBasedPersistentProperty<RelationalPersistentProperty>
		implements RelationalPersistentProperty {

	private static final Map<Class<?>, Class<?>> javaToDbType = new LinkedHashMap<>();

	static {

		javaToDbType.put(Enum.class, String.class);
		javaToDbType.put(ZonedDateTime.class, String.class);
		javaToDbType.put(Temporal.class, Date.class);
	}

	private final RelationalMappingContext context;
	private final Lazy<Optional<String>> columnName;
	private final Lazy<Optional<String>> collectionIdColumnName;
	private final Lazy<Optional<String>> collectionKeyColumnName;
	private final Lazy<Boolean> isEmbedded;
	private final Lazy<String> embeddedPrefix;
	private final Lazy<Class<?>> columnType = Lazy.of(this::doGetColumnType);

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

		this.isEmbedded = Lazy.of(() -> Optional.ofNullable(findAnnotation(Embedded.class)).isPresent());

		this.embeddedPrefix = Lazy.of(() -> Optional.ofNullable(findAnnotation(Embedded.class)) //
				.map(Embedded::value) //
				.orElse(""));

		this.columnName = Lazy.of(() -> Optional.ofNullable(findAnnotation(Column.class)) //
				.map(Column::value) //
				.filter(StringUtils::hasText));

		this.collectionIdColumnName = Lazy.of(() -> Optionals
				.toStream(Optional.ofNullable(findAnnotation(MappedCollection.class)).map(MappedCollection::idColumn),
						Optional.ofNullable(findAnnotation(Column.class)).map(Column::value)) //
				.filter(StringUtils::hasText).findFirst());

		this.collectionKeyColumnName = Lazy.of(() -> Optionals
				.toStream(Optional.ofNullable(findAnnotation(MappedCollection.class)).map(MappedCollection::keyColumn),
						Optional.ofNullable(findAnnotation(Column.class)).map(Column::keyColumn)) //
				.filter(StringUtils::hasText).findFirst());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<RelationalPersistentProperty> createAssociation() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEntity() {
		return super.isEntity() && !isReference();
	}

	@Override
	public boolean isReference() {
		return false;
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
	@Override
	public Class<?> getColumnType() {
		return columnType.get();
	}

	private Class<?> doGetColumnType() {

		if (isReference()) {
			return columnTypeForReference();
		}

		Class columnType = columnTypeIfEntity(getActualType());

		if (columnType != null) {
			return columnType;
		}

		Class componentColumnType = columnTypeForNonEntity(getActualType());

		while (componentColumnType.isArray()) {
			componentColumnType = componentColumnType.getComponentType();
		}

		if (isCollectionLike() && !isEntity()) {
			return Array.newInstance(componentColumnType, 0).getClass();
		}

		return componentColumnType;
	}

	@Override
	public int getSqlType() {
		return -1;
	}

	@Override
	public RelationalPersistentEntity<?> getOwner() {
		return (RelationalPersistentEntity<?>) super.getOwner();
	}

	@Override
	public String getReverseColumnName() {
		return collectionIdColumnName.get().orElseGet(() -> context.getNamingStrategy().getReverseColumnName(this));
	}

	@Override
	public String getKeyColumn() {

		if (isQualified()) {
			return collectionKeyColumnName.get().orElseGet(() -> context.getNamingStrategy().getKeyColumn(this));
		} else {
			return null;
		}
	}

	@Override
	public boolean isQualified() {
		return isMap() || isListLike();
	}

	@Override
	public boolean isOrdered() {
		return isListLike();
	}

	@Override
	public boolean isEmbedded() {
		return isEmbedded.get();
	}

	@Override
	public String getEmbeddedPrefix() {
		return isEmbedded() ? embeddedPrefix.get() : null;
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

	private Class columnTypeForReference() {

		Class<?> componentType = getTypeInformation().getRequiredComponentType().getType();
		RelationalPersistentEntity<?> referencedEntity = context.getRequiredPersistentEntity(componentType);

		return referencedEntity.getRequiredIdProperty().getColumnType();
	}

}
