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
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public class ColumnDetector extends TableAccessor {

	private final AggregatePath path;

	private ColumnDetector(AggregatePath path) {
		super(path);
		this.path = path;
	}

	public static ColumnDetector of(AggregatePath path) {
		return new ColumnDetector(path);
	}

	public static ColumnDetector of(TableAccessor tableOwner) {
		if (tableOwner instanceof ColumnDetector) {
			return (ColumnDetector) tableOwner;
		}

		return new ColumnDetector(tableOwner.getPath());
	}

	@Override
	ColumnDetector createTableAccessor(AggregatePath path) {
		return of(path);
	}

	@Override
	public ColumnDetector getTableOwner() {
		return (ColumnDetector) super.getTableOwner();
	}

	/**
	 * The column name of the id column of the ancestor path that represents an actual table.
	 */
	public SqlIdentifier getIdColumnName() {
		return getTableOwner().getPath().getRequiredLeafEntity().getIdColumn();
	}

	/**
	 * If the table owning ancestor has an id the column name of that id property is returned. Otherwise the reverse
	 * column is returned.
	 */
	public SqlIdentifier getEffectiveIdColumnName() {

		AggregatePath owner = getTableOwner().getPath();
		return owner.isRoot() ? owner.getRequiredLeafEntity().getIdColumn()
				: SingleColumnAggregatePath.of(owner).getReverseColumnName();
	}

	/**
	 * The name of the column used to represent this property in the database.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getColumnName() {

		Assert.state(!path.isRoot(), "Path is null");

		return assembleColumnName(path, path.getRequiredLeafProperty().getColumnName());
	}

	/**
	 * The alias for the column used to represent this property in the database.
	 */
	public SqlIdentifier getColumnAlias() {
		return prefixWithTableAlias(getColumnName());
	}

	private static SqlIdentifier assembleColumnName(AggregatePath path, SqlIdentifier suffix) {

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

}
