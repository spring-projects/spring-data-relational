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
import org.springframework.util.StringUtils;

public final class AggregatePathUtil {

	private AggregatePathUtil() {
		throw new IllegalStateException("This class should never get instantiated");
	}

	public static boolean hasIdProperty(AggregatePath path) {

		RelationalPersistentEntity<?> leafEntity = path.getLeafEntity();
		return leafEntity != null && leafEntity.hasIdProperty();
	}

	/**
	 * Returns the longest ancestor path that has an {@link org.springframework.data.annotation.Id} property.
	 *
	 * @return A path that starts just as this path but is shorter. Guaranteed to be not {@literal null}.
	 */
	public static AggregatePath getIdDefiningParentPath(AggregatePath path) {

		// TODO: What if the path is a root and the filter method returns null?
		return path.getParentPath().filter(AggregatePathUtil::hasIdProperty);
	}

	/**
	 * If the table owning ancestor has an id the column name of that id property is returned. Otherwise the reverse
	 * column is returned.
	 */
	public static SqlIdentifier getEffectiveIdColumnName(AggregatePath path) {

		AggregatePath owner = getTableOwningAncestor(path);
		return owner.isRoot() ? owner.getRequiredLeafEntity().getIdColumn() : getReverseColumnName(owner);
	}

	/**
	 * The alias used in select for the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public static SqlIdentifier getReverseColumnNameAlias(AggregatePath path) {

		return prefixWithTableAlias(path, getReverseColumnName(path));
	}

	/**
	 * The name of the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public static SqlIdentifier getReverseColumnName(AggregatePath path) {

		Assert.state(!path.isRoot(), "Empty paths don't have a reverse column name");

		return path.getRequiredLeafProperty().getReverseColumnName(path);
	}

	/**
	 * Finds and returns the longest path with ich identical or an ancestor to the current path and maps directly to a
	 * table.
	 *
	 * @return a path. Guaranteed to be not {@literal null}.
	 */
	private static AggregatePath getTableOwningAncestor(AggregatePath path) {
		return path.isEntity() && !path.isEmbedded() ? path : getTableOwningAncestor(path.getParentPath());
	}

	@Nullable
	private static SqlIdentifier assembleTableAlias(AggregatePath path) {

		Assert.state(!path.isRoot(), "Path is null");

		RelationalPersistentProperty leafProperty = path.getRequiredLeafProperty();
		String prefix;
		if (path.isEmbedded()) {
			prefix = leafProperty.getEmbeddedPrefix();

		} else {
			prefix = leafProperty.getName();
		}

		if (path.getLength() == 1) {
			Assert.notNull(prefix, "Prefix mus not be null");
			return StringUtils.hasText(prefix) ? SqlIdentifier.quoted(prefix) : null;
		}

		AggregatePath parentPath = path.getParentPath();
		SqlIdentifier sqlIdentifier = assembleTableAlias(parentPath);

		if (sqlIdentifier != null) {

			return parentPath.isEmbedded() ? sqlIdentifier.transform(name -> name.concat(prefix))
					: sqlIdentifier.transform(name -> name + "_" + prefix);
		}
		return SqlIdentifier.quoted(prefix);

	}

	/**
	 * The alias used for the table on which this path is based.
	 *
	 * @return a table alias, {@literal null} if the table owning path is the empty path.
	 */
	@Nullable
	private static SqlIdentifier findTableAlias(AggregatePath path) {

		AggregatePath tableOwner = getTableOwningAncestor(path);

		return tableOwner.isRoot() ? null : assembleTableAlias(tableOwner);

	}

	private static SqlIdentifier assembleColumnName(AggregatePath path, SqlIdentifier suffix) {

		Assert.state(!path.isRoot(), "Path is null");

		if (path.getLength() <= 1) {
			return suffix;
		}

		PersistentPropertyPath<? extends RelationalPersistentProperty> parentPath = path.getParentPath()
				.getRequiredPersistentPropertyPath();
		RelationalPersistentProperty parentLeaf = parentPath.getLeafProperty();

		if (!parentLeaf.isEmbedded()) {
			return suffix;
		}

		String embeddedPrefix = parentLeaf.getEmbeddedPrefix();

		return assembleColumnName(path.getParentPath(), suffix.transform(embeddedPrefix::concat));
	}

	private static SqlIdentifier prefixWithTableAlias(AggregatePath path, SqlIdentifier columnName) {

		SqlIdentifier tableAlias = findTableAlias(path);
		return tableAlias == null ? columnName : columnName.transform(name -> tableAlias.getReference() + "_" + name);
	}

	public static boolean isWritable(@Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return path == null || path.getLeafProperty().isWritable() && isWritable(path.getParentPath());
	}
}
