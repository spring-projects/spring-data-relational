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

import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Mark Paluch
 */
public class TableAccessor {

	private final AggregatePath path;

	TableAccessor(AggregatePath path) {
		this.path = path;
	}

	public static TableAccessor of(AggregatePath path) {
		return new TableAccessor(path);
	}

	/**
	 * The alias used for the table on which this path is based.
	 *
	 * @return a table alias, {@literal null} if the table owning path is the empty path.
	 */
	public boolean hasTableAlias() {

		TableAccessor tableOwner = getTableOwner();
		AggregatePath path = tableOwner.getPath();

		if (path.isRoot()) {
			return false;
		}

		// TODO: Do this better
		return assembleTableAlias(path) != null;
	}

	/**
	 * The fully qualified name of the table this path is tied to or of the longest ancestor path that is actually tied to
	 * a table.
	 *
	 * @return the name of the table. Guaranteed to be not {@literal null}.
	 * @since 3.0
	 */
	public SqlIdentifier getQualifiedTableName() {
		return getTableOwner().getPath().getRequiredLeafEntity().getQualifiedTableName();
	}

	/**
	 * The alias used for the table on which this path is based.
	 *
	 * @return a table alias, {@literal null} if the table owning path is the empty path.
	 */
	@Nullable
	public SqlIdentifier findTableAlias() {

		// TODO: Make non-nullable
		TableAccessor tableOwner = getTableOwner();
		AggregatePath path = tableOwner.getPath();

		return path.isRoot() ? null : assembleTableAlias(path);
	}

	public TableAccessor getTableOwner() {

		AggregatePath result = path.filter(it -> it.isEntity() && !it.isEmbedded());

		if (result == null) {
			throw new IllegalArgumentException("Cannot find table-owning AggregatePath for %s".formatted(path));
		}

		if (result == path) {
			return this;
		}

		return createTableAccessor(result);
	}

	TableAccessor createTableAccessor(AggregatePath path) {
		return new TableAccessor(path);
	}

	public AggregatePath getPath() {
		return path;
	}

	SqlIdentifier prefixWithTableAlias(SqlIdentifier columnName) {

		SqlIdentifier tableAlias = findTableAlias();
		return tableAlias == null ? columnName : columnName.transform(name -> tableAlias.getReference() + "_" + name);
	}

	@Nullable
	private static SqlIdentifier assembleTableAlias(AggregatePath path) {

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

}
