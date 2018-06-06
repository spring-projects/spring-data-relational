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
package org.springframework.data.jdbc.core.function;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.util.Pair;
import org.springframework.util.ClassUtils;

import com.nebhale.r2dbc.spi.Row;
import com.nebhale.r2dbc.spi.RowMetadata;

/**
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final EntityInstantiators instantiators = new EntityInstantiators();
	private final JdbcMappingContext mappingContext = new JdbcMappingContext();

	@Override
	public List<Pair<String, Object>> getInsert(Object object) {

		Class<?> userClass = ClassUtils.getUserClass(object);

		JdbcPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(userClass);
		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		List<Pair<String, Object>> values = new ArrayList<>();

		for (JdbcPersistentProperty property : entity) {

			Object value = propertyAccessor.getProperty(property);

			if (value == null) {
				continue;
			}

			values.add(Pair.of(property.getColumnName(), value));
		}

		return values;
	}

	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<T>((JdbcPersistentEntity) mappingContext.getRequiredPersistentEntity(typeToRead),
				instantiators, mappingContext);
	}

	@Override
	public String getTableName(Class<?> type) {
		return mappingContext.getRequiredPersistentEntity(type).getTableName();
	}
}
