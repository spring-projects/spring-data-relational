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
package org.springframework.data.jdbc.repository;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;

/**
 * @author Jens Schauder
 */
class EntityRowMapper<T> implements org.springframework.jdbc.core.RowMapper<T> {

	private final PersistentEntity<T, ?> entity;

	EntityRowMapper(PersistentEntity<T, ?> entity) {
		this.entity = entity;
	}

	@Override
	public T mapRow(ResultSet rs, int rowNum) throws SQLException {

		try {

			T t = createInstance();

			entity.doWithProperties((PropertyHandler) property -> {
				setProperty(rs, t, property);
			});

			return t;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Could not instantiate %s", entity.getType()));
		}
	}

	private T createInstance() throws InstantiationException, IllegalAccessException, InvocationTargetException {
		return (T) entity.getPersistenceConstructor().getConstructor().newInstance();
	}

	private void setProperty(ResultSet rs, T t, PersistentProperty property) {

		try {
			property.getSetter().invoke(t, rs.getObject(property.getName()));
		} catch (Exception e) {
			throw new RuntimeException(String.format("Couldn't set property %s.", property.getName()), e);
		}
	}
}