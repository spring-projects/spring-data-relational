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
package org.springframework.data.jdbc.repository.support;

import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.repository.query.JdbcEntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.lang.Nullable;

import com.sun.corba.se.spi.ior.ObjectId;

/**
 * {@link JdbcEntityInformation} implementation using a {@link JdbcPersistentEntity} instance to lookup the necessary
 * information. Can be configured with a custom table name.
 *
 * @author Mark Paluch
 */
public class MappingJdbcEntityInformation<T, ID> extends PersistentEntityInformation<T, ID>
		implements JdbcEntityInformation<T, ID> {

	private final JdbcPersistentEntity<T> entityMetadata;
	private final @Nullable String customTableName;
	private final Class<ID> fallbackIdType;

	/**
	 * Creates a new {@link MappingJdbcEntityInformation} for the given {@link JdbcPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public MappingJdbcEntityInformation(JdbcPersistentEntity<T> entity) {
		this(entity, null, null);
	}

	/**
	 * Creates a new {@link MappingJdbcEntityInformation} for the given {@link JdbcPersistentEntity} and fallback
	 * identifier type.
	 *
	 * @param entity must not be {@literal null}.
	 * @param fallbackIdType can be {@literal null}.
	 */
	public MappingJdbcEntityInformation(JdbcPersistentEntity<T> entity, @Nullable Class<ID> fallbackIdType) {
		this(entity, null, fallbackIdType);
	}

	/**
	 * Creates a new {@link MappingJdbcEntityInformation} for the given {@link JdbcPersistentEntity} and custom table
	 * name.
	 *
	 * @param entity must not be {@literal null}.
	 * @param customTableName can be {@literal null}.
	 */
	public MappingJdbcEntityInformation(JdbcPersistentEntity<T> entity, String customTableName) {
		this(entity, customTableName, null);
	}

	/**
	 * Creates a new {@link MappingJdbcEntityInformation} for the given {@link JdbcPersistentEntity}, collection name and
	 * identifier type.
	 *
	 * @param entity must not be {@literal null}.
	 * @param customTableName can be {@literal null}.
	 * @param idType can be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private MappingJdbcEntityInformation(JdbcPersistentEntity<T> entity, @Nullable String customTableName,
			@Nullable Class<ID> idType) {

		super(entity);

		this.entityMetadata = entity;
		this.customTableName = customTableName;
		this.fallbackIdType = idType != null ? idType : (Class<ID>) ObjectId.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.jdbc.repository.query.JdbcEntityInformation#getTableName()
	 */
	public String getTableName() {
		return customTableName == null ? entityMetadata.getTableName() : customTableName;
	}

	public String getIdAttribute() {
		return entityMetadata.getRequiredIdProperty().getName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.PersistentEntityInformation#getIdType()
	 */
	@Override
	public Class<ID> getIdType() {

		if (this.entityMetadata.hasIdProperty()) {
			return super.getIdType();
		}

		return fallbackIdType;
	}
}
