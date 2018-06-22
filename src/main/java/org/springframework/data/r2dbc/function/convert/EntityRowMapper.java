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
package org.springframework.data.r2dbc.function.convert;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.sql.ResultSet;
import java.util.function.BiFunction;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.util.ClassUtils;

/**
 * Maps a {@link io.r2dbc.spi.Row} to an entity of type {@code T}, including entities referenced.
 *
 * @author Mark Paluch
 * @since 1.0
 */
public class EntityRowMapper<T> implements BiFunction<Row, RowMetadata, T> {

	private final JdbcPersistentEntity<T> entity;
	private final EntityInstantiators entityInstantiators;
	private final ConversionService conversions;
	private final MappingContext<JdbcPersistentEntity<?>, JdbcPersistentProperty> context;

	public EntityRowMapper(JdbcPersistentEntity<T> entity, EntityInstantiators entityInstantiators,
			JdbcMappingContext context) {

		this.entity = entity;
		this.entityInstantiators = entityInstantiators;
		this.conversions = context.getConversions();
		this.context = context;
	}

	@Override
	public T apply(Row row, RowMetadata metadata) {

		T result = createInstance(row, "", entity);

		ConvertingPropertyAccessor propertyAccessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(result),
				conversions);

		for (JdbcPersistentProperty property : entity) {

			if (entity.isConstructorArgument(property)) {
				continue;
			}

			if (property.isCollectionLike()) {
				throw new UnsupportedOperationException();
			} else if (property.isMap()) {

				throw new UnsupportedOperationException();
			} else {
				propertyAccessor.setProperty(property, readFrom(row, property, ""));
			}
		}

		return result;
	}

	/**
	 * Read a single value or a complete Entity from the {@link ResultSet} passed as an argument.
	 *
	 * @param row the {@link Row} to extract the value from. Must not be {@literal null}.
	 * @param property the {@link JdbcPersistentProperty} for which the value is intended. Must not be {@literal null}.
	 * @param prefix to be used for all column names accessed by this method. Must not be {@literal null}.
	 * @return the value read from the {@link ResultSet}. May be {@literal null}.
	 */
	private Object readFrom(Row row, JdbcPersistentProperty property, String prefix) {

		try {

			if (property.isEntity()) {
				return readEntityFrom(row, property);
			}

			return row.get(prefix + property.getColumnName(), getType(property));

		} catch (Exception o_O) {
			throw new MappingException(String.format("Could not read property %s from result set!", property), o_O);
		}
	}

	private static Class<?> getType(JdbcPersistentProperty property) {
		return ClassUtils.resolvePrimitiveIfNecessary(property.getActualType());
	}

	private <S> S readEntityFrom(Row row, PersistentProperty<?> property) {

		String prefix = property.getName() + "_";

		@SuppressWarnings("unchecked")
		JdbcPersistentEntity<S> entity = (JdbcPersistentEntity<S>) context
				.getRequiredPersistentEntity(property.getActualType());

		if (readFrom(row, entity.getRequiredIdProperty(), prefix) == null) {
			return null;
		}

		S instance = createInstance(row, prefix, entity);

		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);
		ConvertingPropertyAccessor propertyAccessor = new ConvertingPropertyAccessor(accessor, conversions);

		for (JdbcPersistentProperty p : entity) {
			if (!entity.isConstructorArgument(property)) {
				propertyAccessor.setProperty(p, readFrom(row, p, prefix));
			}
		}

		return instance;
	}

	private <S> S createInstance(Row row, String prefix, JdbcPersistentEntity<S> entity) {

		return entityInstantiators.getInstantiatorFor(entity).createInstance(entity,
				new RowParameterValueProvider(row, entity, conversions, prefix));
	}

	@RequiredArgsConstructor
	private static class RowParameterValueProvider implements ParameterValueProvider<JdbcPersistentProperty> {

		private final @NonNull Row resultSet;
		private final @NonNull JdbcPersistentEntity<?> entity;
		private final @NonNull ConversionService conversionService;
		private final @NonNull String prefix;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		public <T> T getParameterValue(Parameter<T, JdbcPersistentProperty> parameter) {

			String column = prefix + entity.getRequiredPersistentProperty(parameter.getName()).getColumnName();

			try {
				return conversionService.convert(resultSet.get(column, parameter.getType().getType()),
						parameter.getType().getType());
			} catch (Exception o_O) {
				throw new MappingException(String.format("Couldn't read column %s from Row.", column), o_O);
			}
		}
	}
}
