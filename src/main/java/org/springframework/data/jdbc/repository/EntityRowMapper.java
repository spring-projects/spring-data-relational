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

import lombok.RequiredArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.ClassGeneratingEntityInstantiator;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.jdbc.core.RowMapper;

/**
 * Maps a ResultSet to an entity of type {@code T}
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @since 2.0
 */
@RequiredArgsConstructor
class EntityRowMapper<T> implements RowMapper<T> {

	private final JdbcPersistentEntity<T> entity;
	private final EntityInstantiator instantiator = new ClassGeneratingEntityInstantiator();
	private final ConversionService conversions;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

		T result = createInstance(resultSet);

		entity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) property -> {

			PersistentPropertyAccessor accessor = entity.getPropertyAccessor(result);
			ConvertingPropertyAccessor propertyAccessor = new ConvertingPropertyAccessor(accessor, conversions);

			propertyAccessor.setProperty(property, readFrom(resultSet, property));
		});

		return result;
	}

	private T createInstance(ResultSet rs) {
		return instantiator.createInstance(entity, ResultSetParameterValueProvider.of(rs, conversions));
	}

	private static Optional<Object> readFrom(ResultSet resultSet, PersistentProperty<?> property) {

		try {
			return Optional.ofNullable(resultSet.getObject(property.getName()));
		} catch (SQLException o_O) {
			throw new MappingException(String.format("Could not read property %s from result set!", property), o_O);
		}
	}

	@RequiredArgsConstructor(staticName = "of")
	private static class ResultSetParameterValueProvider implements ParameterValueProvider<JdbcPersistentProperty> {

		private final ResultSet resultSet;
		private final ConversionService conversionService;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		public <S> Optional<S> getParameterValue(Parameter<S, JdbcPersistentProperty> parameter) {

			return parameter.getName().map(name -> {

				try {
					return conversionService.convert(resultSet.getObject(name), parameter.getType().getType());
				} catch (SQLException o_O) {
					throw new MappingException(String.format("Couldn't read column %s from ResultSet.", name), o_O);
				}
			});
		}
	}
}
