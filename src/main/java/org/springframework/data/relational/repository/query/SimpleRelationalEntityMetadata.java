/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import lombok.Getter;

import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RelationalEntityMetadata}.
 *
 * @author Mark Paluch
 */
public class SimpleRelationalEntityMetadata<T> implements RelationalEntityMetadata<T> {

	private final Class<T> type;
	private final @Getter JdbcPersistentEntity<?> tableEntity;

	/**
	 * Creates a new {@link SimpleRelationalEntityMetadata} using the given type and {@link JdbcPersistentEntity} to use
	 * for table lookups.
	 *
	 * @param type must not be {@literal null}.
	 * @param tableEntity must not be {@literal null}.
	 */
	public SimpleRelationalEntityMetadata(Class<T> type, JdbcPersistentEntity<?> tableEntity) {

		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(tableEntity, "Table entity must not be null!");

		this.type = type;
		this.tableEntity = tableEntity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityMetadata#getJavaType()
	 */
	public Class<T> getJavaType() {
		return type;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.jdbc.repository.query.JdbcEntityMetadata#getTableName()
	 */
	public String getTableName() {
		return tableEntity.getTableName();
	}
}
