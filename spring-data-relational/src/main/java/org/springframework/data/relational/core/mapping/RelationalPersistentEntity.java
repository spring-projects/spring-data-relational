/*
 * Copyright 2017-2024 the original author or authors.
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

import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * A {@link org.springframework.data.mapping.PersistentEntity} interface with additional methods for JDBC/RDBMS related
 * metadata.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public interface RelationalPersistentEntity<T> extends MutablePersistentEntity<T, RelationalPersistentProperty> {

	/**
	 * Returns the unqualified name of the table (i.e. without schema or owner) backing the given entity.
	 *
	 * @return the table name.
	 */
	SqlIdentifier getTableName();

	/**
	 * Returns the qualified name of the table backing the given entity, including the schema.
	 *
	 * @return the table name including the schema if there is any specified.
	 * @since 3.0
	 */
	default SqlIdentifier getQualifiedTableName() {
		return getTableName();
	}

	/**
	 * Returns the column representing the identifier.
	 *
	 * @return will never be {@literal null}.
	 */
	SqlIdentifier getIdColumn();

}
