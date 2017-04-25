/*
 * Copyright 2017 the original author or authors.
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

import java.io.Serializable;
import java.util.Optional;

import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

/**
 * @author Jens Schauder
 * @since 2.0
 */
public class BasicJdbcPersistentEntityInformation<T, ID extends Serializable> extends PersistentEntityInformation<T, ID>
		implements JdbcPersistentEntityInformation<T, ID> {

	private final JdbcPersistentEntity<T> persistentEntity;

	public BasicJdbcPersistentEntityInformation(JdbcPersistentEntity<T> persistentEntity) {

		super(persistentEntity);

		this.persistentEntity = persistentEntity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation#setId(java.lang.Object, java.util.Optional)
	 */
	@Override
	public void setId(T instance, Optional<Object> value) {
		persistentEntity.getPropertyAccessor(instance).setProperty(persistentEntity.getRequiredIdProperty(), value);
	}
}
