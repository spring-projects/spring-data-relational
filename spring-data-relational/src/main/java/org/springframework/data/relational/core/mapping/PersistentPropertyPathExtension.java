/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.Objects;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A wrapper around a {@link org.springframework.data.mapping.PersistentPropertyPath} for making common operations
 * available used in SQL generation and conversion
 *
 * @author Jens Schauder
 * @author Daniil Razorenov
 * @author Kurt Niemi
 * @since 1.1
 * @deprecated use {@link AggregatePath} instead
 */
@Deprecated(since = "3.2", forRemoval = true)
public class PersistentPropertyPathExtension {

	private final RelationalPersistentEntity<?> entity;
	private final @Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context;

	private final Lazy<SqlIdentifier> columnAlias = Lazy.of(() -> prefixWithTableAlias(getColumnName()));

	/**
	 * Creates the empty path referencing the root itself.
	 *
	 * @param context Must not be {@literal null}.
	 * @param entity Root entity of the path. Must not be {@literal null}.
	 */
	public PersistentPropertyPathExtension(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			RelationalPersistentEntity<?> entity) {

		Assert.notNull(context, "Context must not be null");
		Assert.notNull(entity, "Entity must not be null");

		this.context = context;
		this.entity = entity;
		this.path = null;
	}

	/**
	 * Creates a non-empty path.
	 *
	 * @param context must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 */
	public PersistentPropertyPathExtension(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		Assert.notNull(context, "Context must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(path.getBaseProperty(), "Path must not be empty.");

		this.context = context;
		this.entity = path.getBaseProperty().getOwner();
		this.path = path;
	}

	public static boolean isWritable(@Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return path == null || path.getLeafProperty().isWritable() && isWritable(path.getParentPath());
	}

	/**
	 * Returns {@literal true} exactly when the path is non-empty and the leaf property an embedded one.
	 *
	 * @return if the leaf property is embedded.
	 */
	public boolean isEmbedded() {
		return path != null && path.getLeafProperty().isEmbedded();
	}

	/**
	 * Returns the path that has the same beginning but is one segment shorter than this path.
	 *
	 * @return the parent path. Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException when called on an empty path.
	 */
	public PersistentPropertyPathExtension getParentPath() {

		if (path == null) {
			throw new IllegalStateException("The parent path of a root path is not defined.");
		}

		if (path.getLength() == 1) {
			return new PersistentPropertyPathExtension(context, entity);
		}

		return new PersistentPropertyPathExtension(context, path.getParentPath());
	}

	/**
	 * Returns {@literal true} if there are multiple values for this path, i.e. if the path contains at least one element
	 * that is a collection and array or a map.
	 *
	 * @return {@literal true} if the path contains a multivalued element.
	 */
	public boolean isMultiValued() {

		return path != null && //
				(path.getLeafProperty().isCollectionLike() //
						|| path.getLeafProperty().isQualified() //
						|| getParentPath().isMultiValued() //
				);
	}

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path.
	 *
	 * @return Might return {@literal null} when called on a path that does not represent an entity.
	 */
	@Nullable
	public RelationalPersistentEntity<?> getLeafEntity() {
		return path == null ? entity : context.getPersistentEntity(path.getLeafProperty().getActualType());
	}

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path or throw {@link IllegalStateException}
	 * if the leaf cannot be resolved.
	 *
	 * @return the required {@link RelationalPersistentEntity} associated with the leaf of this path.
	 * @since 3.0
	 * @throws IllegalStateException if the persistent entity cannot be resolved.
	 */
	public RelationalPersistentEntity<?> getRequiredLeafEntity() {

		RelationalPersistentEntity<?> entity = getLeafEntity();

		if (entity == null) {

			if (this.path == null) {
				throw new IllegalStateException("Couldn't resolve leaf PersistentEntity absent path");
			}
			throw new IllegalStateException(
					String.format("Couldn't resolve leaf PersistentEntity for type %s", path.getLeafProperty().getActualType()));
		}

		return entity;
	}

	/**
	 * @return {@literal true} when this is an empty path or the path references an entity.
	 */
	public boolean isEntity() {
		return path == null || path.getLeafProperty().isEntity();
	}

	/**
	 * @return {@literal true} when this is references a {@link java.util.List} or {@link java.util.Map}.
	 */
	public boolean isQualified() {
		return path != null && path.getLeafProperty().isQualified();
	}

	/**
	 * @return {@literal true} when this is references a {@link java.util.Collection} or an array.
	 */
	public boolean isCollectionLike() {
		return path != null && path.getLeafProperty().isCollectionLike();
	}

	/**
	 * The name of the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getReverseColumnName() {

		Assert.state(path != null, "Empty paths don't have a reverse column name");
		return path.getLeafProperty().getReverseColumnName(this);
	}

	/**
	 * The alias used in select for the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getReverseColumnNameAlias() {

		return prefixWithTableAlias(getReverseColumnName());
	}

	/**
	 * The name of the column used to represent this property in the database.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getColumnName() {

		Assert.state(path != null, "Path is null");

		return path.getLeafProperty().getColumnName();
	}

	/**
	 * The alias for the column used to represent this property in the database.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getColumnAlias() {
		return columnAlias.get();
	}

	/**
	 * @return {@literal true} if this path represents an entity which has an Id attribute.
	 */
	public boolean hasIdProperty() {

		RelationalPersistentEntity<?> leafEntity = getLeafEntity();
		return leafEntity != null && leafEntity.hasIdProperty();
	}

	/**
	 * Returns the longest ancestor path that has an {@link org.springframework.data.annotation.Id} property.
	 *
	 * @return A path that starts just as this path but is shorter. Guaranteed to be not {@literal null}.
	 */
	public PersistentPropertyPathExtension getIdDefiningParentPath() {

		PersistentPropertyPathExtension parent = getParentPath();

		if (parent.path == null) {
			return parent;
		}

		if (!parent.hasIdProperty()) {
			return parent.getIdDefiningParentPath();
		}

		return parent;
	}

	/**
	 * The fully qualified name of the table this path is tied to or of the longest ancestor path that is actually tied to
	 * a table.
	 *
	 * @return the name of the table. Guaranteed to be not {@literal null}.
	 * @since 3.0
	 */
	public SqlIdentifier getQualifiedTableName() {
		return getTableOwningAncestor().getRequiredLeafEntity().getQualifiedTableName();
	}

	/**
	 * The name of the table this path is tied to or of the longest ancestor path that is actually tied to a table.
	 *
	 * @return the name of the table. Guaranteed to be not {@literal null}.
	 * @since 3.0
	 */
	public SqlIdentifier getTableName() {
		return getTableOwningAncestor().getRequiredLeafEntity().getTableName();
	}

	/**
	 * The alias used for the table on which this path is based.
	 *
	 * @return a table alias, {@literal null} if the table owning path is the empty path.
	 */
	@Nullable
	public SqlIdentifier getTableAlias() {

		PersistentPropertyPathExtension tableOwner = getTableOwningAncestor();

		return tableOwner.path == null ? null : tableOwner.assembleTableAlias();

	}

	/**
	 * The column name of the id column of the ancestor path that represents an actual table.
	 */
	public SqlIdentifier getIdColumnName() {
		return getTableOwningAncestor().getRequiredLeafEntity().getIdColumn();
	}

	/**
	 * If the table owning ancestor has an id the column name of that id property is returned. Otherwise the reverse
	 * column is returned.
	 */
	public SqlIdentifier getEffectiveIdColumnName() {

		PersistentPropertyPathExtension owner = getTableOwningAncestor();
		return owner.path == null ? owner.getRequiredLeafEntity().getIdColumn() : owner.getReverseColumnName();
	}

	/**
	 * The length of the path.
	 */
	public int getLength() {
		return path == null ? 0 : path.getLength();
	}

	/**
	 * Tests if {@code this} and the argument represent the same path.
	 *
	 * @param path to which this path gets compared. May be {@literal null}.
	 * @return Whence the argument matches the path represented by this instance.
	 */
	public boolean matches(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return this.path == null ? path.isEmpty() : this.path.equals(path);
	}

	/**
	 * The id property of the final element of the path.
	 *
	 * @return Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException if no such property exists.
	 */
	public RelationalPersistentProperty getRequiredIdProperty() {
		return this.path == null ? entity.getRequiredIdProperty() : getRequiredLeafEntity().getRequiredIdProperty();
	}

	/**
	 * The column name used for the list index or map key of the leaf property of this path.
	 *
	 * @return May be {@literal null}.
	 */
	@Nullable
	public SqlIdentifier getQualifierColumn() {
		return path == null ? SqlIdentifier.EMPTY : path.getLeafProperty().getKeyColumn();
	}

	/**
	 * The type of the qualifier column of the leaf property of this path or {@literal null} if this is not applicable.
	 *
	 * @return may be {@literal null}.
	 */
	@Nullable
	public Class<?> getQualifierColumnType() {
		return path == null ? null : path.getLeafProperty().getQualifierColumnType();
	}

	/**
	 * Creates a new path by extending the current path by the property passed as an argument.
	 *
	 * @param property must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	public PersistentPropertyPathExtension extendBy(RelationalPersistentProperty property) {

		PersistentPropertyPath<? extends RelationalPersistentProperty> newPath = path == null //
				? context.getPersistentPropertyPath(property.getName(), entity.getType()) //
				: context.getPersistentPropertyPath(path.toDotPath() + "." + property.getName(), entity.getType());

		return new PersistentPropertyPathExtension(context, newPath);
	}

	@Override
	public String toString() {
		return String.format("PersistentPropertyPathExtension[%s, %s]", entity.getName(),
				path == null ? "-" : path.toDotPath());
	}

	/**
	 * For empty paths this is the type of the entity. For non empty paths this is the actual type of the leaf property.
	 *
	 * @return Guaranteed to be not {@literal null}.
	 * @see PersistentProperty#getActualType()
	 */
	public Class<?> getActualType() {

		return path == null //
				? entity.getType() //
				: path.getLeafProperty().getActualType();
	}

	/**
	 * @return {@literal true} if the leaf property of this path is a {@link java.util.Map}.
	 * @see RelationalPersistentProperty#isMap()
	 */
	public boolean isMap() {
		return path != null && path.getLeafProperty().isMap();
	}

	/**
	 * Converts this path to a non-null {@link PersistentPropertyPath}.
	 *
	 * @return Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException if this path is empty.
	 */
	public PersistentPropertyPath<? extends RelationalPersistentProperty> getRequiredPersistentPropertyPath() {

		Assert.state(path != null, "No path.");

		return path;
	}

	/**
	 * Finds and returns the longest path with ich identical or an ancestor to the current path and maps directly to a
	 * table.
	 *
	 * @return a path. Guaranteed to be not {@literal null}.
	 */
	private PersistentPropertyPathExtension getTableOwningAncestor() {

		return isEntity() && !isEmbedded() ? this : getParentPath().getTableOwningAncestor();
	}

	@Nullable
	private SqlIdentifier assembleTableAlias() {

		Assert.state(path != null, "Path is null");

		RelationalPersistentProperty leafProperty = path.getLeafProperty();
		String prefix;
		if (isEmbedded()) {
			prefix = leafProperty.getEmbeddedPrefix();

		} else {
			prefix = leafProperty.getName();
		}

		if (path.getLength() == 1) {
			Assert.notNull(prefix, "Prefix mus not be null");
			return StringUtils.hasText(prefix) ? SqlIdentifier.quoted(prefix) : null;
		}

		PersistentPropertyPathExtension parentPath = getParentPath();
		SqlIdentifier sqlIdentifier = parentPath.assembleTableAlias();

		if (sqlIdentifier != null) {

			return parentPath.isEmbedded() ? sqlIdentifier.transform(name -> name.concat(prefix))
					: sqlIdentifier.transform(name -> name + "_" + prefix);
		}
		return SqlIdentifier.quoted(prefix);

	}

	private SqlIdentifier prefixWithTableAlias(SqlIdentifier columnName) {

		SqlIdentifier tableAlias = getTableAlias();
		return tableAlias == null ? columnName : columnName.transform(name -> tableAlias.getReference() + "_" + name);
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		PersistentPropertyPathExtension that = (PersistentPropertyPathExtension) o;
		return entity.equals(that.entity) && Objects.equals(path, that.path);
	}

	@Override
	public int hashCode() {
		return Objects.hash(entity, path);
	}

	public AggregatePath getAggregatePath() {
		if (path != null) {

			return ((RelationalMappingContext) context).getAggregatePath(path);
		} else {
			return ((RelationalMappingContext) context).getAggregatePath(entity);
		}
	}
}
