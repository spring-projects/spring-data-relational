/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core;

import lombok.NonNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.ClassGeneratingEntityInstantiator;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.jdbc.core.RowMapper;

/**
 * Maps a ResultSet to an entity of type {@code T}, including entities referenced.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @since 2.0
 */
public class EntityRowMapper<T> implements RowMapper<T> {

	private final JdbcPersistentEntity<T> entity;
	private final EntityInstantiator instantiator = new ClassGeneratingEntityInstantiator();
	private final ConversionService conversions;
	private final JdbcMappingContext context;
	private final DataAccessStrategy accessStrategy;
	private final JdbcPersistentProperty idProperty;

	public EntityRowMapper(JdbcPersistentEntity<T> entity, ConversionService conversions, JdbcMappingContext context,
			DataAccessStrategy accessStrategy) {

		this.entity = entity;
		this.conversions = conversions;
		this.context = context;
		this.accessStrategy = accessStrategy;

		idProperty = entity.getRequiredIdProperty();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
	 */
	@Override
	public T mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

		T result = createInstance(resultSet);

		ConvertingPropertyAccessor propertyAccessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(result),
				conversions);

		Object id = readFrom(resultSet, idProperty, "");

		for (JdbcPersistentProperty property : entity) {

			if (Set.class.isAssignableFrom(property.getType())) {
				propertyAccessor.setProperty(property, accessStrategy.findAllByProperty(id, property));
			} else if (Map.class.isAssignableFrom(property.getType())) {

				Iterable<Object> allByProperty = accessStrategy.findAllByProperty(id, property);
				IterableOfEntryToMapConverter converter = new IterableOfEntryToMapConverter();
				propertyAccessor.setProperty(property, converter.convert(allByProperty));
			} else {
				propertyAccessor.setProperty(property, readFrom(resultSet, property, ""));
			}
		}

		return result;
	}

	private T createInstance(ResultSet rs) {
		return instantiator.createInstance(entity, ResultSetParameterValueProvider.of(rs, conversions, ""));
	}

	private Object readFrom(ResultSet resultSet, JdbcPersistentProperty property, String prefix) {

		try {

			if (property.isEntity()) {
				return readEntityFrom(resultSet, property);
			}

			return resultSet.getObject(prefix + property.getColumnName());
		} catch (SQLException o_O) {
			throw new MappingException(String.format("Could not read property %s from result set!", property), o_O);
		}
	}

	private <S> S readEntityFrom(ResultSet rs, PersistentProperty<?> property) {

		String prefix = property.getName() + "_";

		@SuppressWarnings("unchecked")
		JdbcPersistentEntity<S> entity = (JdbcPersistentEntity<S>) context
				.getRequiredPersistentEntity(property.getActualType());

		if (readFrom(rs, entity.getRequiredIdProperty(), prefix) == null) {
			return null;
		}

		S instance = instantiator.createInstance(entity, ResultSetParameterValueProvider.of(rs, conversions, prefix));

		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);
		ConvertingPropertyAccessor propertyAccessor = new ConvertingPropertyAccessor(accessor, conversions);

		for (JdbcPersistentProperty p : entity) {
			propertyAccessor.setProperty(p, readFrom(rs, p, prefix));
		}

		return instance;
	}

	private static class ResultSetParameterValueProvider implements ParameterValueProvider<JdbcPersistentProperty> {

		@NonNull private final ResultSet resultSet;
		@NonNull private final ConversionService conversionService;
		@NonNull private final String prefix;

		private ResultSetParameterValueProvider(ResultSet resultSet, ConversionService conversionService, String prefix) {

			this.resultSet = resultSet;
			this.conversionService = conversionService;
			this.prefix = prefix;
		}

		public static ResultSetParameterValueProvider of(ResultSet resultSet, ConversionService conversionService,
				String prefix) {
			return new ResultSetParameterValueProvider(resultSet, conversionService, prefix);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		public <T> T getParameterValue(Parameter<T, JdbcPersistentProperty> parameter) {

			String name = parameter.getName();
			if (name == null) {
				return null;
			}

			String column = prefix + name;
			try {
				return conversionService.convert(resultSet.getObject(column), parameter.getType().getType());
			} catch (SQLException o_O) {
				throw new MappingException(String.format("Couldn't read column %s from ResultSet.", column), o_O);
			}
		}
	}
}
