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
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
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
	 * @since 3.5
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

	@Nullable
	AggregatePath getTail();

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

			ColumnInfos reverseColumnInfos,

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
			ColumnInfos idColumnInfos) {

		static TableInfo of(AggregatePath path) {

			AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);

			RelationalPersistentEntity<?> leafEntity = tableOwner.getRequiredLeafEntity();
			SqlIdentifier qualifiedTableName = leafEntity.getQualifiedTableName();

			SqlIdentifier tableAlias = tableOwner.isRoot() ? null : AggregatePathTableUtils.constructTableAlias(tableOwner);

			ColumnInfos reverseColumnInfos = computeReverseColumnInfo(path);

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

			return new TableInfo(qualifiedTableName, tableAlias, reverseColumnInfos, qualifierColumnInfo, qualifierColumnType,
					idColumnInfos);

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

		private static ColumnInfos computeReverseColumnInfo(AggregatePath path) {

			AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);

			if (tableOwner.isRoot()) {
				return ColumnInfos.empty(tableOwner);
			}

			AggregatePath idDefiningParentPath = tableOwner.getIdDefiningParentPath();
			RelationalPersistentProperty leafProperty = tableOwner.getRequiredLeafProperty();

			RelationalPersistentProperty idProperty = idDefiningParentPath.getLeafEntity().getIdProperty();

			if (idProperty != null) {
				if (idProperty.isEntity()) {

					AggregatePath idBasePath = idDefiningParentPath.append(idProperty);
					ColumInfosBuilder ciBuilder = new ColumInfosBuilder(idBasePath);

					RelationalPersistentEntity<?> idEntity = idBasePath.getRequiredLeafEntity();
					idEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
						AggregatePath idElementPath = idBasePath.append(p);
						SqlIdentifier name = idElementPath.getColumnInfo().name();
						name = name.transform(n -> idDefiningParentPath.getTableInfo().qualifiedTableName.getReference() + "_" + n);

						ciBuilder.add(idElementPath, name, name);
					});

					return ciBuilder.build();

				} else {

					ColumInfosBuilder ciBuilder = new ColumInfosBuilder(idDefiningParentPath);
					SqlIdentifier reverseColumnName = leafProperty
							.getReverseColumnName(idDefiningParentPath.getRequiredLeafEntity());

					ciBuilder.add(idProperty, reverseColumnName,
							AggregatePathTableUtils.prefixWithTableAlias(path, reverseColumnName));

					return ciBuilder.build();
				}
			} else {

				ColumInfosBuilder ciBuilder = new ColumInfosBuilder(idDefiningParentPath);
				SqlIdentifier reverseColumnName = leafProperty
						.getReverseColumnName(idDefiningParentPath.getRequiredLeafEntity());

				ciBuilder.add(idDefiningParentPath, reverseColumnName,
						AggregatePathTableUtils.prefixWithTableAlias(path, reverseColumnName));

				return ciBuilder.build();
			}

		}

		@Deprecated(forRemoval = true)
		public ColumnInfo reverseColumnInfo() {
			return reverseColumnInfos.unique();
		}

		public ColumnInfos effectiveIdColumnInfos() {
			return reverseColumnInfos.columnInfos.isEmpty() ? idColumnInfos : reverseColumnInfos;
		}
	}

	/**
	 * @param name The name of the column used to represent this property in the database.
	 * @param alias The alias for the column used to represent this property in the database.
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
	 * A group of {@link ColumnInfo} values referenced by there respective {@link AggregatePath}. This is relevant for
	 * composite ids and references to such ids.
	 **/
	class ColumnInfos {

		private final AggregatePath basePath;
		private final Map<AggregatePath, ColumnInfo> columnInfos;

		/**
		 * @param basePath The path on which all other paths in the other argument are based on. For the typical case of a
		 *          composite id, this would be the path to the composite ids.
		 * @param columnInfos A map, mapping {@literal AggregatePath} instances to the respective {@literal ColumnInfo}
		 */
		private ColumnInfos(AggregatePath basePath, Map<AggregatePath, ColumnInfo> columnInfos) {

			this.basePath = basePath;
			this.columnInfos = columnInfos;
		}

		public static ColumnInfos empty(AggregatePath base) {
			return new ColumnInfos(base, new HashMap<>());
		}

		public ColumnInfo unique() {

			Collection<ColumnInfo> values = columnInfos.values();
			Assert.state(values.size() == 1, "ColumnInfo is not unique");
			return values.iterator().next();
		}

		public ColumnInfo any() {

			Collection<ColumnInfo> values = columnInfos.values();
			return values.iterator().next();
		}

		public boolean isEmpty() {
			return columnInfos.isEmpty();
		}

		public <T> List<T> toList(Function<ColumnInfo, T> mapper) {
			return columnInfos.values().stream().map(mapper).toList();
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
		 * @return result of the function.
		 * @param <T> type of the result.
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

		public void forEach(BiConsumer<AggregatePath, ColumnInfo> consumer) {
			columnInfos.forEach(consumer);
		}

		public <T> T any(BiFunction<AggregatePath, ColumnInfo, T> mapper) {

			Map.Entry<AggregatePath, ColumnInfo> any = columnInfos.entrySet().iterator().next();
			return mapper.apply(any.getKey(), any.getValue());
		}

		public ColumnInfo get(AggregatePath path) {
			return columnInfos.get(path);
		}

		public AggregatePath fullPath(AggregatePath ap) {
			return basePath.append(ap);
		}

		public int size() {
			return columnInfos.size();
		}
	}

	class ColumInfosBuilder {
		private final AggregatePath basePath;

		private final Map<AggregatePath, ColumnInfo> columnInfoMap = new TreeMap<>();

		public ColumInfosBuilder(AggregatePath basePath) {
			this.basePath = basePath;
		}

		void add(AggregatePath path, SqlIdentifier name, SqlIdentifier alias) {
			add(path, new ColumnInfo(name, alias));
		}

		public void add(RelationalPersistentProperty property, SqlIdentifier name, SqlIdentifier alias) {
			add(basePath.append(property), name, alias);
		}

		ColumnInfos build() {
			return new ColumnInfos(basePath, columnInfoMap);
		}

		public void add(AggregatePath path, ColumnInfo columnInfo) {
			columnInfoMap.put(path.substract(basePath), columnInfo);
		}
	}

	@Nullable
	AggregatePath substract(@Nullable AggregatePath basePath);

}
