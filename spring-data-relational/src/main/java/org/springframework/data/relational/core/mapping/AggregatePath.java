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

import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a path within an aggregate starting from the aggregate root. The path can be iterated from the leaf to its
 * root.
 *
 * @since 3.2
 * @author Jens Schauder
 * @author Mark Paluch
 */
public interface AggregatePath extends Iterable<AggregatePath> {

	/**
	 * Returns the path that has the same beginning but is one segment shorter than this path.
	 *
	 * @return the parent path. Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException when called on an empty path.
	 */
	AggregatePath getParentPath();

	/**
	 * Creates a new path by extending the current path by the property passed as an argument.
	 *
	 * @param property must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	AggregatePath append(RelationalPersistentProperty property);

	/**
	 * @return {@literal true} if this is a root path for the underlying type.
	 */
	boolean isRoot();

	/**
	 * Returns the path length for the aggregate path.
	 *
	 * @return the path length for the aggregate path
	 */
	default int getLength() {
		return 1 + (isRoot() ? 0 : getRequiredPersistentPropertyPath().getLength());
	}

	boolean isWritable();

	/**
	 * @return {@literal true} when this is an empty path or the path references an entity.
	 */
	boolean isEntity();

	/**
	 * Returns {@literal true} exactly when the path is non-empty and the leaf property an embedded one.
	 *
	 * @return if the leaf property is embedded.
	 */
	boolean isEmbedded();

	/**
	 * Returns {@literal true} if there are multiple values for this path, i.e. if the path contains at least one element
	 * that is a collection and array or a map. // TODO: why does this return true if the parent entity is a collection?
	 * This seems to mix some concepts that belong to somewhere else. // TODO: Multi-valued could be understood for
	 * embeddables with more than one column (i.e. composite primary keys)
	 *
	 * @return {@literal true} if the path contains a multivalued element.
	 */
	boolean isMultiValued();

	/**
	 * @return {@literal true} when this is references a {@link java.util.List} or {@link java.util.Map}.
	 */
	boolean isQualified();

	/**
	 * @return {@literal true} if the leaf property of this path is a {@link java.util.Map}.
	 * @see RelationalPersistentProperty#isMap()
	 */
	boolean isMap();

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
	 * @return {@literal true} if this path represents an entity which has an identifier attribute.
	 */
	boolean hasIdProperty();

	RelationalPersistentProperty getRequiredIdProperty();

	/**
	 * @return the persistent property path if the path is not a {@link #isRoot() root} path.
	 * @throws IllegalStateException if the current path is a {@link #isRoot() root} path.
	 * @see PersistentPropertyPath#getBaseProperty()
	 */
	PersistentPropertyPath<RelationalPersistentProperty> getRequiredPersistentPropertyPath();

	/**
	 * @return the base property.
	 * @throws IllegalStateException if the current path is a {@link #isRoot() root} path.
	 * @see PersistentPropertyPath#getBaseProperty()
	 */
	default RelationalPersistentProperty getRequiredBaseProperty() {
		return getRequiredPersistentPropertyPath().getBaseProperty();
	}

	/**
	 * @return the leaf property.
	 * @throws IllegalStateException if the current path is a {@link #isRoot() root} path.
	 * @see PersistentPropertyPath#getLeafProperty()
	 */
	default RelationalPersistentProperty getRequiredLeafProperty() {
		return getRequiredPersistentPropertyPath().getLeafProperty();
	}

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
	default RelationalPersistentEntity<?> getRequiredLeafEntity() {

		RelationalPersistentEntity<?> entity = getLeafEntity();

		if (entity == null) {

			throw new IllegalStateException(String.format("Couldn't resolve leaf PersistentEntity for type %s",
					getRequiredLeafProperty().getActualType()));
		}

		return entity;
	}

	/**
	 * Returns the dot based path notation using {@link PersistentProperty#getName()}.
	 *
	 * @return will never be {@literal null}.
	 */
	String toDotPath();

	// TODO: Conceptually, AggregatePath works with properties. The mapping into columns and tables should reside in a
	// utility that can distinguish whether a property maps to one or many columns (e.g. embedded) and the same for
	// identifier columns.
	default TableInfo getTableInfo() {
		return TableInfo.of(this);
	}

	default ColumnInfo getColumnInfo() {
		return ColumnInfo.of(this);
	}

	/**
	 * Filter the AggregatePath returning the first item matching the given {@link Predicate}.
	 *
	 * @param predicate must not be {@literal null}.
	 * @return the first matching element or {@literal null}.
	 */
	@Nullable
	default AggregatePath filter(Predicate<? super AggregatePath> predicate) {

		for (AggregatePath item : this) {
			if (predicate.test(item)) {
				return item;
			}
		}

		return null;
	}

	/**
	 * Creates a non-parallel {@link Stream} of the underlying {@link Iterable}.
	 *
	 * @return will never be {@literal null}.
	 */
	default Stream<AggregatePath> stream() {
		return StreamSupport.stream(spliterator(), false);
	}

	// path navigation

	/**
	 * Returns the longest ancestor path that has an {@link org.springframework.data.annotation.Id} property.
	 *
	 * @return A path that starts just as this path but is shorter. Guaranteed to be not {@literal null}. TODO: throws
	 *         NoSuchElementException: No value present for empty paths
	 */
	AggregatePath getIdDefiningParentPath();

	record TableInfo(

			/*
			 * The fully qualified name of the table this path is tied to or of the longest ancestor path that is actually
			 * tied to a table.
			 */
			SqlIdentifier qualifiedTableName,

			/*
			 * The alias used for the table on which this path is based.
			 */
			@Nullable SqlIdentifier tableAlias,

			ColumnInfo reverseColumnInfo,

			/*
			 * The column used for the list index or map key of the leaf property of this path.
			 */
			@Nullable ColumnInfo qualifierColumnInfo,

			/*
			 * The type of the qualifier column of the leaf property of this path or {@literal null} if this is not
			 * applicable.
			 */
			@Nullable Class<?> qualifierColumnType,

			/*
			 * The column name of the id column of the ancestor path that represents an actual table.
			 */
			SqlIdentifier idColumnName,

			/*
			 * If the table owning ancestor has an id the column name of that id property is returned. Otherwise the reverse
			 * column is returned.
			 */
			SqlIdentifier effectiveIdColumnName) {

		static TableInfo of(AggregatePath path) {

			AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);

			RelationalPersistentEntity<?> leafEntity = tableOwner.getRequiredLeafEntity();
			SqlIdentifier qualifiedTableName = leafEntity.getQualifiedTableName();

			SqlIdentifier tableAlias = tableOwner.isRoot() ? null : AggregatePathTableUtils.constructTableAlias(tableOwner);

			ColumnInfo reverseColumnInfo = null;
			if (!tableOwner.isRoot()) {

				AggregatePath idDefiningParentPath = tableOwner.getIdDefiningParentPath();
				RelationalPersistentProperty leafProperty = tableOwner.getRequiredLeafProperty();

				SqlIdentifier reverseColumnName = leafProperty
						.getReverseColumnName(idDefiningParentPath.getRequiredLeafEntity());

				reverseColumnInfo = new ColumnInfo(reverseColumnName,
						AggregatePathTableUtils.prefixWithTableAlias(path, reverseColumnName));
			}

			ColumnInfo qualifierColumnInfo = null;
			if (!path.isRoot()) {

				SqlIdentifier keyColumn = path.getRequiredLeafProperty().getKeyColumn();
				if (keyColumn != null) {
					qualifierColumnInfo = new ColumnInfo(keyColumn, keyColumn);
				}
			}

			Class<?> qualifierColumnType = null;
			if (!path.isRoot() && path.getRequiredLeafProperty().isQualified()) {
				qualifierColumnType = path.getRequiredLeafProperty().getQualifierColumnType();
			}

			SqlIdentifier idColumnName = leafEntity.hasIdProperty() ? leafEntity.getIdColumn() : null;

			SqlIdentifier effectiveIdColumnName = tableOwner.isRoot() ? idColumnName : reverseColumnInfo.name();

			return new TableInfo(qualifiedTableName, tableAlias, reverseColumnInfo, qualifierColumnInfo, qualifierColumnType,
					idColumnName, effectiveIdColumnName);

		}
	}

	record ColumnInfo(

			/* The name of the column used to represent this property in the database. */
			SqlIdentifier name, /* The alias for the column used to represent this property in the database. */
			SqlIdentifier alias) {

		/**
		 * Create a {@link ColumnInfo} from an aggregate path. ColumnInfo can be created for simple type single-value
		 * properties only.
		 *
		 * @param path
		 * @return the {@link ColumnInfo}.
		 * @throws IllegalArgumentException if the path is {@link #isRoot()}, {@link #isEmbedded()} or
		 *           {@link #isMultiValued()}.
		 */
		static ColumnInfo of(AggregatePath path) {

			Assert.notNull(path, "AggregatePath must not be null");
			Assert.isTrue(!path.isRoot(), () -> "Cannot obtain ColumnInfo for root path");
			Assert.isTrue(!path.isEmbedded(), () -> "Cannot obtain ColumnInfo for embedded path");

			// TODO: Multi-valued paths cannot be represented with a single column
			// Assert.isTrue(!path.isMultiValued(), () -> "Cannot obtain ColumnInfo for multi-valued path");

			SqlIdentifier columnName = path.getRequiredLeafProperty().getColumnName();
			return new ColumnInfo(columnName, AggregatePathTableUtils.prefixWithTableAlias(path, columnName));
		}
	}
}
