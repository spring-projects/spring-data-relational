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
package org.springframework.data.jdbc.mapping.model;

import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;

/**
 * meta data a repository might need for implementing persistence operations for instances of type {@code T}
 * @author Jens Schauder
 */
public class JdbcPersistentEntity<T> extends BasicPersistentEntity<T, JdbcPersistentProperty> {

	private String tableName;
	private String idColumn;

	public JdbcPersistentEntity(TypeInformation<T> information) {
		super(information);
	}

	public String getTableName() {

		if (tableName == null)
			tableName = getType().getSimpleName();

		return tableName;
	}

	public String getIdColumn() {

		if (idColumn == null)
			idColumn = getIdProperty().getName();

		return idColumn;
	}

	public Object getIdValue(T instance) {
		return getPropertyAccessor(instance).getProperty(getIdProperty());
	}

	public void setId(T instance, Object value) {
		getPropertyAccessor(instance).setProperty(getIdProperty(),value);
	}
}
