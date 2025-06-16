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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.util.Lazy;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentLruCache;

/**
 * Represents a path within an aggregate starting from the aggregate root.
 *
 * @since 3.2
 * @author Jens Schauder
 * @author Mark Paluch
 */
class DefaultAggregatePath implements AggregatePath {

	private final RelationalMappingContext context;

	private final @Nullable RelationalPersistentEntity<?> rootType;

	private final @Nullable PersistentPropertyPath<RelationalPersistentProperty> path;

	private final Lazy<TableInfo> tableInfo = Lazy.of(() -> TableInfo.of(this));

	private final Lazy<ColumnInfo> columnInfo = Lazy.of(() -> ColumnInfo.of(this));

	private final ConcurrentLruCache<RelationalPersistentProperty, AggregatePath> nestedCache;

	@SuppressWarnings("unchecked")
	DefaultAggregatePath(RelationalMappingContext context,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		Assert.notNull(context, "context must not be null");
		Assert.notNull(path, "path must not be null");

		this.context = context;
		this.path = (PersistentPropertyPath) path;
		this.rootType = path.getBaseProperty().getOwner();
		this.nestedCache = new ConcurrentLruCache<>(32, this::doGetAggegatePath);
	}

	DefaultAggregatePath(RelationalMappingContext context, RelationalPersistentEntity<?> rootType) {

		Assert.notNull(context, "context must not be null");
		Assert.notNull(rootType, "rootType must not be null");

		this.context = context;
		this.rootType = rootType;
		this.path = null;
		this.nestedCache = new ConcurrentLruCache<>(32, this::doGetAggegatePath);
	}

	/**
	 * Returns the path that has the same beginning but is one segment shorter than this path.
	 *
	 * @return the parent path. Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException when called on an empty path.
	 */
	@Override
	public AggregatePath getParentPath() {

		if (isRoot()) {
			throw new IllegalStateException("The parent path of a root path is not defined.");
		}

		PersistentPropertyPath<RelationalPersistentProperty> path = getRequiredPersistentPropertyPath();

		if (path.getLength() == 1) {
			return context.getAggregatePath(path.getLeafProperty().getOwner());
		}

		return context.getAggregatePath(path.getParentPath());
	}

	@Override
	public AggregatePath append(RelationalPersistentProperty property) {
		return nestedCache.get(property);
	}

	@Override
	public AggregatePath append(AggregatePath path) {

		if (path.isRoot()) {
			return this;
		}

		RelationalPersistentProperty baseProperty = path.getRequiredBaseProperty();
		AggregatePath appended = append(baseProperty);
		AggregatePath tail = path.getTail();
		return tail == null ? appended : appended.append(tail);

	}

	private AggregatePath doGetAggegatePath(RelationalPersistentProperty property) {

		PersistentPropertyPath<? extends RelationalPersistentProperty> newPath = isRoot() //
				? context.getPersistentPropertyPath(property.getName(), rootType.getTypeInformation()) //
				: context.getPersistentPropertyPath(path.toDotPath() + "." + property.getName(),
						path.getBaseProperty().getOwner().getTypeInformation());

		return context.getAggregatePath(newPath);
	}

	@Override
	public boolean isRoot() {
		return path == null;
	}

	@Override
	public boolean isWritable() {
		return stream().allMatch(path -> path.isRoot() || path.getRequiredLeafProperty().isWritable());
	}

	@Override
	public boolean isEntity() {
		return isRoot() || getRequiredLeafProperty().isEntity();
	}

	@Override
	public boolean isEmbedded() {
		return !isRoot() && getRequiredLeafProperty().isEmbedded();
	}

	@Override
	public boolean isMultiValued() {

		return !isRoot() && //
				(getRequiredLeafProperty().isCollectionLike() //
						|| getRequiredLeafProperty().isQualified() //
						// TODO: Considering the parent as multi-valued burries the scope of this method.
						// this needs to be resolved
						|| getParentPath().isMultiValued() //
				);
	}

	@Override
	public boolean isQualified() {
		return !isRoot() && getRequiredLeafProperty().isQualified();
	}

	@Override
	public boolean isMap() {
		return !isRoot() && getRequiredLeafProperty().isMap();
	}

	@Override
	public boolean isCollectionLike() {
		return !isRoot() && getRequiredLeafProperty().isCollectionLike();
	}

	@Override
	public boolean isOrdered() {
		return !isRoot() && getRequiredLeafProperty().isOrdered();
	}

	@Override
	public boolean hasIdProperty() {

		RelationalPersistentEntity<?> leafEntity = getLeafEntity();
		return leafEntity != null && leafEntity.hasIdProperty();
	}

	@Override
	public RelationalPersistentProperty getRequiredIdProperty() {
		return isRoot() ? rootType.getRequiredIdProperty() : getRequiredLeafEntity().getRequiredIdProperty();
	}

	@Override
	public PersistentPropertyPath<RelationalPersistentProperty> getRequiredPersistentPropertyPath() {

		Assert.state(path != null, "Root Aggregate Paths are not associated with a PersistentPropertyPath");
		return path;
	}

	@Override
	public RelationalPersistentEntity<?> getLeafEntity() {
		return isRoot() ? rootType
				: context.getPersistentEntity(getRequiredLeafProperty().getTypeInformation().getActualType());
	}

	@Override
	public String toDotPath() {
		return isRoot() ? "" : getRequiredPersistentPropertyPath().toDotPath();
	}

	@Override
	public AggregatePath getIdDefiningParentPath() {
		return AggregatePathTraversal.getIdDefiningPath(this);
	}

	@Override
	public AggregatePath getTail() {

		if (getLength() <= 2) {
			return null;
		}

		AggregatePath tail = null;
		for (RelationalPersistentProperty prop : this.path) {
			if (tail == null) {
				tail = context.getAggregatePath(context.getPersistentEntity(prop));
			} else {
				tail = tail.append(prop);
			}
		}
		return tail;
	}

	@Override
	@Nullable
	public AggregatePath substract(@Nullable AggregatePath basePath) {

		if (basePath == null || basePath.isRoot()) {
			return this;
		}

		if (this.isRoot()) {
			throw new IllegalStateException("Can't subtract from root path");
		}

		if (basePath.getRequiredBaseProperty().equals(getRequiredBaseProperty())) {
			AggregatePath tail = this.getTail();
			if (tail == null) {
				return null;
			}
			return tail.substract(basePath.getTail());
		}

		throw new IllegalStateException("Can't subtract [%s] from [%s]".formatted(basePath, this));
	}

	/**
	 * Finds and returns the longest path with ich identical or an ancestor to the current path and maps directly to a
	 * table.
	 *
	 * @return a path. Guaranteed to be not {@literal null}.
	 */
	private AggregatePath getTableOwningAncestor() {
		return AggregatePathTraversal.getTableOwningPath(this);
	}

	/**
	 * Creates an {@link Iterator} that iterates over the current path and all ancestors. It will start with the current
	 * path, followed by its parent until ending with the root.
	 */
	@Override
	public Iterator<AggregatePath> iterator() {
		return new AggregatePathIterator(this);
	}

	@Override
	public TableInfo getTableInfo() {
		return this.tableInfo.get();
	}

	@Override
	public ColumnInfo getColumnInfo() {
		return this.columnInfo.get();
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		DefaultAggregatePath that = (DefaultAggregatePath) o;
		return Objects.equals(context, that.context) && Objects.equals(rootType, that.rootType)
				&& Objects.equals(path, that.path);
	}

	@Override
	public int hashCode() {
		return Objects.hash(context, rootType, path);
	}

	@Override
	public String toString() {
		return "AggregatePath["
				+ (rootType == null ? path.getBaseProperty().getOwner().getType().getName() : rootType.getName()) + "]"
				+ ((isRoot()) ? "/" : path.toDotPath());
	}

	@Override
	public int compareTo(@NonNull AggregatePath other) {
		return toDotPath().compareTo(other.toDotPath());
	}

	private static class AggregatePathIterator implements Iterator<AggregatePath> {

		private @Nullable AggregatePath current;

		public AggregatePathIterator(AggregatePath current) {
			this.current = current;
		}

		@Override
		public boolean hasNext() {
			return current != null;
		}

		@Override
		public AggregatePath next() {

			AggregatePath element = current;

			if (element == null) {
				throw new NoSuchElementException();
			}

			current = element.isRoot() ? null : element.getParentPath();

			return element;
		}
	}

}
