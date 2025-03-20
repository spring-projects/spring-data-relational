/*
 * Copyright 2023-2025 the original author or authors.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a path within an aggregate starting from the aggregate root. The path can be iterated from the leaf to its
 * root.
 * <p>
 * It implements {@link Comparable} so that collections of {@code AggregatePath} instances can be sorted in a consistent
 * way.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 3.2
 */
public interface AggregatePath extends Iterable<AggregatePath>, Comparable<AggregatePath> {

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
	 * Creates a new path by extending the current path by the path passed as an argument.
	 *
	 * @param path must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 * @since 4.0
	 */
	AggregatePath append(AggregatePath path);

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

	/**
	 * The path resulting from removing the first element of the {@link AggregatePath}.
	 *
	 * @return {@literal null} for any {@link AggregatePath} having less than two elements.
	 * @since 4.0
	 */
	@Nullable
	AggregatePath getTail();

	/**
	 * Subtract the {@literal basePath} from {@literal this} {@literal AggregatePath} by removing the {@literal basePath}
	 * from the beginning of {@literal this}.
	 *
	 * @param basePath the path to be removed.
	 * @return an AggregatePath that ends like the original {@literal AggregatePath} but has {@literal basePath} removed
	 *         from the beginning.
	 * @since 4.0
	 */
	@Nullable
	AggregatePath subtract(@Nullable AggregatePath basePath);

	/**
	 * Compares this {@code AggregatePath} to another {@code AggregatePath} based on their dot path notation.
	 * <p>
	 * This is used to get {@code AggregatePath} instances sorted in a consistent way. Since this order affects generated
	 * SQL this also affects query caches and similar.
	 *
	 * @param other the {@code AggregatePath} to compare to. Must not be {@literal null}.
	 * @return a negative integer, zero, or a positive integer as this object's path is less than, equal to, or greater
	 *         than the specified object's path.
	 * @since 4.0
	 */
	@Override
	default int compareTo(AggregatePath other) {
		return toDotPath().compareTo(other.toDotPath());
	}

	/**
	 * Information about a table underlying an entity.
	 *
	 * @param qualifiedTableName the fully qualified name of the table this path is tied to or of the longest ancestor
	 *          path that is actually tied to a table. Must not be {@literal null}.
	 * @param tableAlias the alias used for the table on which this path is based. May be {@literal null}.
	 * @param backReferenceColumnInfos information about the columns used to reference back to the owning entity. Must not
	 *          be {@literal null}. Since 3.5.
	 * @param qualifierColumnInfo the column used for the list index or map key of the leaf property of this path. May be
	 *          {@literal null}.
	 * @param qualifierColumnType the type of the qualifier column of the leaf property of this path or {@literal null} if
	 *          this is not applicable. May be {@literal null}.
	 * @param idColumnInfos the column name of the id column of the ancestor path that represents an actual table. Must
	 *          not be {@literal null}.
	 */
	record TableInfo(SqlIdentifier qualifiedTableName, @Nullable SqlIdentifier tableAlias,
			ColumnInfos backReferenceColumnInfos, @Nullable ColumnInfo qualifierColumnInfo,
			@Nullable Class<?> qualifierColumnType, ColumnInfos idColumnInfos) {

		static TableInfo of(AggregatePath path) {

			AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);

			RelationalPersistentEntity<?> leafEntity = tableOwner.getRequiredLeafEntity();
			SqlIdentifier qualifiedTableName = leafEntity.getQualifiedTableName();

			SqlIdentifier tableAlias = tableOwner.isRoot() ? null : AggregatePathTableUtils.constructTableAlias(tableOwner);

			ColumnInfos backReferenceColumnInfos = computeBackReferenceColumnInfos(path);

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

			ColumnInfos idColumnInfos = computeIdColumnInfos(tableOwner, leafEntity);

			return new TableInfo(qualifiedTableName, tableAlias, backReferenceColumnInfos, qualifierColumnInfo,
					qualifierColumnType, idColumnInfos);

		}

		private static ColumnInfos computeIdColumnInfos(AggregatePath tableOwner,
				RelationalPersistentEntity<?> leafEntity) {

			ColumnInfos idColumnInfos = ColumnInfos.empty(tableOwner);
			if (!leafEntity.hasIdProperty()) {
				return idColumnInfos;
			}

			RelationalPersistentProperty idProperty = leafEntity.getRequiredIdProperty();
			AggregatePath idPath = tableOwner.append(idProperty);

			if (idProperty.isEntity()) {
				ColumInfosBuilder ciBuilder = new ColumInfosBuilder(idPath);
				idPath.getRequiredLeafEntity().doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
					AggregatePath idElementPath = idPath.append(p);
					ciBuilder.add(idElementPath, ColumnInfo.of(idElementPath));
				});
				return ciBuilder.build();
			} else {
				ColumInfosBuilder ciBuilder = new ColumInfosBuilder(idPath.getParentPath());
				ciBuilder.add(idPath, ColumnInfo.of(idPath));
				return ciBuilder.build();
			}
		}

		private static ColumnInfos computeBackReferenceColumnInfos(AggregatePath path) {

			AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);

			if (tableOwner.isRoot()) {
				return ColumnInfos.empty(tableOwner);
			}

			AggregatePath idDefiningParentPath = tableOwner.getIdDefiningParentPath();
			RelationalPersistentProperty idProperty = idDefiningParentPath.getRequiredLeafEntity().getIdProperty();

			AggregatePath basePath = idProperty != null && idProperty.isEntity() ? idDefiningParentPath.append(idProperty)
					: idDefiningParentPath;
			ColumInfosBuilder ciBuilder = new ColumInfosBuilder(basePath);

			if (idProperty != null && idProperty.isEntity()) {

				RelationalPersistentEntity<?> idEntity = basePath.getRequiredLeafEntity();
				idEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
					AggregatePath idElementPath = basePath.append(p);
					SqlIdentifier name = idElementPath.getColumnInfo().name();
					name = name.transform(n -> idDefiningParentPath.getTableInfo().qualifiedTableName.getReference() + "_" + n);

					ciBuilder.add(idElementPath, name, name);
				});

			} else {

				RelationalPersistentProperty leafProperty = tableOwner.getRequiredLeafProperty();
				SqlIdentifier reverseColumnName = leafProperty
						.getReverseColumnName(idDefiningParentPath.getRequiredLeafEntity());
				SqlIdentifier alias = AggregatePathTableUtils.prefixWithTableAlias(path, reverseColumnName);

				if (idProperty != null) {
					ciBuilder.add(idProperty, reverseColumnName, alias);
				} else {
					ciBuilder.add(idDefiningParentPath, reverseColumnName, alias);
				}
			}
			return ciBuilder.build();
		}

		@Override
		public ColumnInfos backReferenceColumnInfos() {
			return backReferenceColumnInfos;
		}

		/**
		 * Returns the unique {@link ColumnInfo} referencing the parent table, if such exists.
		 *
		 * @return guaranteed not to be {@literal null}.
		 * @throws IllegalStateException if there is not exactly one back referencing column.
		 * @deprecated since there might be more than one reverse column instead. Use {@link #backReferenceColumnInfos()}
		 *             instead.
		 */
		@Deprecated(forRemoval = true)
		public ColumnInfo reverseColumnInfo() {
			return backReferenceColumnInfos.unique();
		}

		/**
		 * The id columns of the underlying table.
		 * <p>
		 * These might be:
		 * <ul>
		 * <li>the columns representing the id of the entity in question.</li>
		 * <li>the columns representing the id of a parent entity, which _owns_ the table. Note that this case also covers
		 * the first case.</li>
		 * <li>or the backReferenceColumns.</li>
		 * </ul>
		 *
		 * @return ColumnInfos representing the effective id of this entity. Guaranteed not to be {@literal null}.
		 */
		public ColumnInfos effectiveIdColumnInfos() {
			return backReferenceColumnInfos.columnInfos.isEmpty() ? idColumnInfos : backReferenceColumnInfos;
		}
	}

	/**
	 * @param name the name of the column used to represent this property in the database.
	 * @param alias the alias for the column used to represent this property in the database.
	 * @since 3.2
	 */
	record ColumnInfo(SqlIdentifier name, SqlIdentifier alias) {

		/**
		 * Create a {@link ColumnInfo} from an aggregate path. ColumnInfo can be created for simple type single-value
		 * properties only.
		 *
		 * @param path the path to the {@literal ColumnInfo} for.
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

	/**
	 * A group of {@link ColumnInfo} values referenced by there respective {@link AggregatePath}. It is used in a similar
	 * way as {@literal ColumnInfo} when one needs to consider more than a single column. This is relevant for composite
	 * ids and references to such ids.
	 *
	 * @author Jens Schauder
	 * @since 4.0
	 */
	class ColumnInfos {

		private final AggregatePath basePath;
		private final Map<AggregatePath, ColumnInfo> columnInfos;
		private final Map<Table, List<Column>> columnCache = new HashMap<>();

		/**
		 * Creates a new ColumnInfos instances based on the arguments.
		 *
		 * @param basePath The path on which all other paths in the other argument are based on. For the typical case of a
		 *          composite id, this would be the path to the composite ids.
		 * @param columnInfos A map, mapping {@literal AggregatePath} instances to the respective {@literal ColumnInfo}
		 */
		ColumnInfos(AggregatePath basePath, Map<AggregatePath, ColumnInfo> columnInfos) {

			this.basePath = basePath;
			this.columnInfos = columnInfos;
		}

		/**
		 * An empty {@literal ColumnInfos} instance with a fixed base path. Useful as a base when collecting
		 * {@link ColumnInfo} instances into an {@literal ColumnInfos} instance.
		 *
		 * @param basePath The path on which paths in the {@literal ColumnInfos} or derived objects will be based on.
		 * @return an empty instance save the {@literal basePath}.
		 */
		public static ColumnInfos empty(AggregatePath basePath) {
			return new ColumnInfos(basePath, new HashMap<>());
		}

		/**
		 * If this instance contains exactly one {@link ColumnInfo} it will be returned.
		 *
		 * @return the unique {@literal ColumnInfo} if present.
		 * @throws IllegalStateException if the number of contained {@literal ColumnInfo} instances is not exactly 1.
		 */
		public ColumnInfo unique() {

			Collection<ColumnInfo> values = columnInfos.values();
			Assert.state(values.size() == 1, "ColumnInfo is not unique");
			return values.iterator().next();
		}

		/**
		 * Any of the contained {@link ColumnInfo} instances.
		 *
		 * @return a {@link ColumnInfo} instance.
		 * @throws java.util.NoSuchElementException if no instance is available.
		 */
		public ColumnInfo any() {

			Collection<ColumnInfo> values = columnInfos.values();
			return values.iterator().next();
		}

		/**
		 * Checks if {@literal this} instance is empty, i.e. does not contain any {@link ColumnInfo} instance.
		 *
		 * @return {@literal true} iff the collection of {@literal ColumnInfo} is empty.
		 */
		public boolean isEmpty() {
			return columnInfos.isEmpty();
		}

		/**
		 * Converts the given {@link Table} into a list of {@link Column}s. This method retrieves and caches the list of
		 * columns for the specified table. If the columns are not already cached, it computes the list by mapping
		 * {@code columnInfos} to their corresponding {@link Column} in the provided table and then stores the result in the
		 * cache.
		 *
		 * @param table the {@link Table} for which the columns should be generated; must not be {@literal null}.
		 * @return a list of {@link Column}s associated with the specified {@link Table}. Guaranteed no to be
		 *         {@literal null}.
		 */
		public List<Column> toColumnList(Table table) {

			return columnCache.computeIfAbsent(table,
					t -> columnInfos.values().stream().map(columnInfo -> t.column(columnInfo.name)).toList());
		}

		/**
		 * Performs a {@link Stream#reduce(Object, BiFunction, BinaryOperator)} on {@link ColumnInfo} and
		 * {@link AggregatePath} to reduce the results into a single {@code T} return value.
		 * <p>
		 * If {@code ColumnInfos} is empty, then {@code identity} is returned. Without invoking {@code combiner}. The
		 * {@link BinaryOperator combiner} is called with the current state (or initial {@code identity}) and the
		 * accumulated {@code T} state to combine both into a single return value.
		 *
		 * @param identity the identity (initial) value for the combiner function.
		 * @param accumulator an associative, non-interfering (free of side effects), stateless function for incorporating
		 *          an additional element into a result.
		 * @param combiner an associative, non-interfering, stateless function for combining two values, which must be
		 *          compatible with the {@code accumulator} function.
		 * @param <T> type of the result.
		 * @return result of the function.
		 * @since 3.5
		 */
		public <T> T reduce(T identity, BiFunction<AggregatePath, ColumnInfo, T> accumulator, BinaryOperator<T> combiner) {

			T result = identity;

			for (Map.Entry<AggregatePath, ColumnInfo> entry : columnInfos.entrySet()) {

				T mapped = accumulator.apply(entry.getKey(), entry.getValue());
				result = combiner.apply(result, mapped);
			}

			return result;
		}

		/**
		 * Calls the consumer for each pair of {@link AggregatePath} and {@literal ColumnInfo}.
		 *
		 * @param consumer the function to call.
		 */
		public void forEach(BiConsumer<AggregatePath, ColumnInfo> consumer) {
			columnInfos.forEach(consumer);
		}

		/**
		 * Calls the {@literal mapper} for each pair one pair of {@link AggregatePath} and {@link ColumnInfo}, if there is
		 * any.
		 *
		 * @param mapper the function to call.
		 * @return the result of the mapper
		 * @throws java.util.NoSuchElementException if this {@literal ColumnInfo} is empty.
		 */
		public <T> T any(BiFunction<AggregatePath, ColumnInfo, T> mapper) {

			Map.Entry<AggregatePath, ColumnInfo> any = columnInfos.entrySet().iterator().next();
			return mapper.apply(any.getKey(), any.getValue());
		}

		/**
		 * Gets the {@link ColumnInfo} for the provided {@link AggregatePath}
		 *
		 * @param path for which to return the {@literal ColumnInfo}
		 * @return {@literal ColumnInfo} for the given path.
		 */
		public ColumnInfo get(AggregatePath path) {
			return columnInfos.get(path);
		}

		/**
		 * Constructs an {@link AggregatePath} from the {@literal basePath} and the provided argument.
		 *
		 * @param ap {@literal AggregatePath} to be appended to the {@literal basePath}.
		 * @return the combined (@literal AggregatePath}
		 */
		public AggregatePath fullPath(AggregatePath ap) {
			return basePath.append(ap);
		}

		/**
		 * Number of {@literal ColumnInfo} elements in this instance.
		 *
		 * @return the size of the collection of {@literal ColumnInfo}.
		 */
		public int size() {
			return columnInfos.size();
		}
	}

}
