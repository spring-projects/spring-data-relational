/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a path within an aggregate starting from the aggregate root.
 *
 * @since 3.2
 * @author Jens Schauder
 */
public interface AggregatePath {

	static boolean isWritable(@Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return path == null || path.getLeafProperty().isWritable() && isWritable(path.getParentPath());
	}

	boolean isRoot();

	/**
	 * The name of the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	SqlIdentifier getReverseColumnName();

	/**
	 * Returns the path that has the same beginning but is one segment shorter than this path.
	 *
	 * @return the parent path. Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException when called on an empty path.
	 */
	AggregatePath getParentPath();

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path.
	 *
	 * @return Might return {@literal null} when called on a path that does not represent an entity.
	 */
	@Nullable
	RelationalPersistentEntity<?> getLeafEntity();

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path or throw {@link IllegalStateException}
	 * if the leaf cannot be resolved.
	 *
	 * @return the required {@link RelationalPersistentEntity} associated with the leaf of this path.
	 * @throws IllegalStateException if the persistent entity cannot be resolved.
	 */
	RelationalPersistentEntity<?> getRequiredLeafEntity();

	/**
	 * @return {@literal true} if this path represents an entity which has an Id attribute.
	 */
	boolean hasIdProperty();

	/**
	 * Returns the longest ancestor path that has an {@link org.springframework.data.annotation.Id} property.
	 *
	 * @return A path that starts just as this path but is shorter. Guaranteed to be not {@literal null}.
	 */
	AggregatePath getIdDefiningParentPath();

	RelationalPersistentProperty getRequiredIdProperty();

	int getLength();

	/**
	 * The column name used for the list index or map key of the leaf property of this path.
	 *
	 * @return May be {@literal null}.
	 */
	@Nullable
	SqlIdentifier getQualifierColumn();

	/**
	 * The type of the qualifier column of the leaf property of this path or {@literal null} if this is not applicable.
	 *
	 * @return may be {@literal null}.
	 */
	@Nullable
	Class<?> getQualifierColumnType();

	/**
	 * Returns {@literal true} exactly when the path is non empty and the leaf property an embedded one.
	 *
	 * @return if the leaf property is embedded.
	 */
	boolean isEmbedded();

	/**
	 * @return {@literal true} when this is an empty path or the path references an entity.
	 */
	boolean isEntity();

	/**
	 * The alias used for the table on which this path is based.
	 *
	 * @return a table alias, {@literal null} if the table owning path is the empty path.
	 */
	@Nullable
	SqlIdentifier getTableAlias();

	/**
	 * The fully qualified name of the table this path is tied to or of the longest ancestor path that is actually tied to
	 * a table.
	 *
	 * @return the name of the table. Guaranteed to be not {@literal null}.
	 * @since 3.0
	 */
	SqlIdentifier getQualifiedTableName();

	/**
	 * The name of the column used to represent this property in the database.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	SqlIdentifier getColumnName();

	/**
	 * The alias for the column used to represent this property in the database.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	SqlIdentifier getColumnAlias();

	/**
	 * The alias used in select for the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	SqlIdentifier getReverseColumnNameAlias();

	String toDotPath();

	/**
	 * Returns {@literal true} if there are multiple values for this path, i.e. if the path contains at least one element
	 * that is a collection and array or a map.
	 *
	 * @return {@literal true} if the path contains a multivalued element.
	 */
	boolean isMultiValued();

	/**
	 * @return {@literal true} if the leaf property of this path is a {@link java.util.Map}.
	 * @see RelationalPersistentProperty#isMap()
	 */
	boolean isMap();

	/**
	 * @return {@literal true} when this is references a {@link java.util.List} or {@link java.util.Map}.
	 */
	boolean isQualified();

	RelationalPersistentProperty getRequiredLeafProperty();

	RelationalPersistentProperty getBaseProperty();

	/**
	 * The column name of the id column of the ancestor path that represents an actual table.
	 */
	SqlIdentifier getIdColumnName();

	/**
	 * @return {@literal true} when this is references a {@link java.util.Collection} or an array.
	 */
	boolean isCollectionLike();

	/**
	 * @return whether the leaf end of the path is ordered, i.e. the data to populate must be ordered.
	 * @see RelationalPersistentProperty#isOrdered()
	 */
	boolean isOrdered();

	/**
	 * Creates a new path by extending the current path by the property passed as an argument.
	 *
	 * @param property must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	AggregatePath append(RelationalPersistentProperty property);

	PersistentPropertyPath<? extends RelationalPersistentProperty> getRequiredPersistentPropertyPath();

	/**
	 * If the table owning ancestor has an id the column name of that id property is returned. Otherwise the reverse
	 * column is returned.
	 */
	SqlIdentifier getEffectiveIdColumnName();

	@Nullable
	PersistentPropertyPathExtension getPathExtension();

	/**
	 * Finds and returns the longest path with ich identical or an ancestor to the current path and maps directly to a
	 * table.
	 *
	 * @return a path. Guaranteed to be not {@literal null}.
	 */
	AggregatePath getTableOwningAncestor();

	@Nullable
	SqlIdentifier assembleTableAlias();

	SqlIdentifier assembleColumnName(SqlIdentifier suffix);
}