/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RelationalEntityMetadata}.
 *
 * @author Mark Paluch
 */
public class SimpleRelationalEntityMetadata<T> implements RelationalEntityMetadata<T> {

	private final Class<T> type;
	private final RelationalPersistentEntity<?> tableEntity;

	/**
	 * Creates a new {@link SimpleRelationalEntityMetadata} using the given type and {@link RelationalPersistentEntity} to
	 * use for table lookups.
	 *
	 * @param type must not be {@literal null}.
	 * @param tableEntity must not be {@literal null}.
	 */
	public SimpleRelationalEntityMetadata(Class<T> type, RelationalPersistentEntity<?> tableEntity) {

		Assert.notNull(type, "Type must not be null");
		Assert.notNull(tableEntity, "Table entity must not be null");

		this.type = type;
		this.tableEntity = tableEntity;
	}

	public Class<T> getJavaType() {
		return type;
	}

	public SqlIdentifier getTableName() {
		return tableEntity.getQualifiedTableName();
	}

	public RelationalPersistentEntity<?> getTableEntity() {
		return this.tableEntity;
	}
}
