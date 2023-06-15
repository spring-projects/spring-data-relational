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
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public class ForeignTableDetector extends TableAccessor{

	private final AggregatePath path;

	ForeignTableDetector(AggregatePath path) {
		super(path);
		this.path = path;
	}

	public static ForeignTableDetector of(AggregatePath path) {

		Assert.notNull(path, "AggregatePath must not be null");

		if (path.isRoot()) {
			throw new IllegalStateException("Root path does not map to a single column");
		}

		if (path.isEmbedded()) {
			throw new IllegalStateException(String.format("Embedded property %s does not map to a foreign table", path));
		}

		if (!path.isQualified()) {
			throw new IllegalStateException(String.format("Property %s does not map to a foreign table", path));
		}

		return new ForeignTableDetector(path);
	}

	/**
	 * The column name used for the list index or map key of the leaf property of this path.
	 *
	 * @throws IllegalStateException if the key column cannot be determined for the current path.
	 */
	public SqlIdentifier getQualifierColumn() {

		SqlIdentifier keyColumn = path.getRequiredLeafProperty().getKeyColumn();

		if (keyColumn == null) {
			throw new IllegalStateException("Cannot determine key column for %s".formatted(path));
		}
		return keyColumn;
	}

	/**
	 * The type of the qualifier column of the leaf property of this path or {@literal null} if this is not applicable.
	 *
	 * @return may be {@literal null}.
	 */
	public Class<?> getQualifierColumnType() {

		RelationalPersistentProperty property = path.getRequiredLeafProperty();

		return property.getQualifierColumnType();
	}

	/**
	 * The name of the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getReverseColumnName() {
		return path.getRequiredLeafProperty().getReverseColumnName(path);
	}

}
