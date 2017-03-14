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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.ClassGeneratingEntityInstantiator;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.jdbc.core.RowMapper;

/**
 * Maps a ResultSet to an entity of type {@code T}
 *
 * @author Jens Schauder
 * @since 2.0
 */
class EntityRowMapper<T> implements RowMapper<T> {

	private final JdbcPersistentEntity<T> entity;
	private final EntityInstantiator instantiator = new ClassGeneratingEntityInstantiator();
	private final ConversionService conversions = new DefaultConversionService();

	EntityRowMapper(JdbcPersistentEntity<T> entity) {
		this.entity = entity;
	}

	@Override
	public T mapRow(ResultSet rs, int rowNum) throws SQLException {

		T t = createInstance(rs);

		entity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) property -> {
			setProperty(rs, t, property);
		});

		return t;
	}

	private T createInstance(ResultSet rs) {

		return instantiator.createInstance(entity, new ParameterValueProvider<JdbcPersistentProperty>() {

			@Override
			public <T> T getParameterValue(PreferredConstructor.Parameter<T, JdbcPersistentProperty> parameter) {

				try {
					return conversions.convert(rs.getObject(parameter.getName()), parameter.getType().getType());
				} catch (SQLException e) {

					throw new MappingException( //
							String.format("Couldn't read column %s from ResultSet.", parameter.getName()) //
					);
				}
			}
		});
	}

	private void setProperty(ResultSet rs, T t, PersistentProperty property) {

		try {

			Object converted = conversions.convert(rs.getObject(property.getName()), property.getType());
			entity.getPropertyAccessor(t).setProperty(property, converted);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Couldn't set property %s.", property.getName()), e);
		}
	}
}
