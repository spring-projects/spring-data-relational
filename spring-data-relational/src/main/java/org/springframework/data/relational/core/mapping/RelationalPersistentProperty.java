/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.mapping;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A {@link PersistentProperty} with methods for additional JDBC/RDBMS related meta data.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
public interface RelationalPersistentProperty extends PersistentProperty<RelationalPersistentProperty> {

	boolean isReference();

	/**
	 * Returns the name of the column backing this property.
	 *
	 * @return the name of the column backing this property.
	 */
	String getColumnName();

	/**
	 * The type to be used to store this property in the database.
	 *
	 * @return a {@link Class} that is suitable for usage with JDBC drivers.
	 */
	Class<?> getColumnType();

	/**
	 * The SQL type constant used when using this property as a parameter for a SQL statement.
	 * 
	 * @return Must not be {@code null}.
	 * @see java.sql.Types
	 */
	int getSqlType();

	@Override
	RelationalPersistentEntity<?> getOwner();

	/**
	 * @return the name of the column referencing back to the owning entities table.
	 * @deprecated use {@link #getReverseColumnName(PersistentPropertyPath, PersistentPropertyPath)} instead, which does
	 *             not assume the owning entity has a unique id column.
	 */
	@Deprecated
	String getReverseColumnName();

	/**
	 * @param referencedPath
	 * @param thisPath
	 * @return the list of column names to reference back to the owning entities table. Guaranteed to be not {@code null}.
	 */
	default String getReverseColumnName(PersistentPropertyPath<RelationalPersistentProperty> referencedPath,
			PersistentPropertyPath<RelationalPersistentProperty> thisPath) {

		if (thisPath == null && referencedPath != null) { // todo temporary workaround. see SqlGenerator callsite.
			return referencedPath.getLeafProperty().getReverseColumnName();
		}

		Assert.notNull(thisPath, "thisPath must end in this property and must not be null.");
		Assert.isTrue(thisPath.getRequiredLeafProperty().equals(this), "thisPath must end in this property.");

		int refLength = (referencedPath == null) ? 0 : referencedPath.getLength();
		int diff = thisPath.getLength() - refLength;

		for (int i = diff; i > 1; i--) {
			thisPath = thisPath.getParentPath();
		}

		return thisPath.getLeafProperty().getReverseColumnName();
	}

	@Nullable
	String getKeyColumn();

	@Nullable
	default String getKeyColumn(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return path.getLeafProperty().getKeyColumn();
	}

	/**
	 * Returns if this property is a qualified property, i.e. a property referencing multiple elements that can get picked
	 * by a key or an index.
	 */
	boolean isQualified();

	/**
	 * Returns whether this property is an ordered property.
	 */
	boolean isOrdered();

}
