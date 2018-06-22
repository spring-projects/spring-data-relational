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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.r2dbc.function.convert.EntityRowMapper;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.ClassUtils;

/**
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final RelationalMappingContext mappingContext;
	private final EntityInstantiators instantiators;

	public DefaultReactiveDataAccessStrategy() {
		this(new RelationalMappingContext(), new EntityInstantiators());
	}

	public DefaultReactiveDataAccessStrategy(RelationalMappingContext mappingContext, EntityInstantiators instantiators) {
		this.mappingContext = mappingContext;
		this.instantiators = instantiators;
	}

	@Override
	public List<String> getAllFields(Class<?> typeToRead) {

		RelationalPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(typeToRead);

		if (persistentEntity == null) {
			return Collections.singletonList("*");
		}

		return StreamUtils.createStreamFromIterator(persistentEntity.iterator()) //
				.map(RelationalPersistentProperty::getColumnName) //
				.collect(Collectors.toList());
	}

	@Override
	public List<Pair<String, Object>> getInsert(Object object) {

		Class<?> userClass = ClassUtils.getUserClass(object);

		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(userClass);
		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		List<Pair<String, Object>> values = new ArrayList<>();

		for (RelationalPersistentProperty property : entity) {

			Object value = propertyAccessor.getProperty(property);

			if (value == null) {
				continue;
			}

			values.add(Pair.of(property.getColumnName(), value));
		}

		return values;
	}

	@Override
	public Sort getMappedSort(Class<?> typeToRead, Sort sort) {

		RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToRead);
		if (entity == null) {
			return sort;
		}

		List<Order> mappedOrder = new ArrayList<>();

		for (Order order : sort) {

			RelationalPersistentProperty persistentProperty = entity.getPersistentProperty(order.getProperty());
			if (persistentProperty == null) {
				mappedOrder.add(order);
			} else {
				mappedOrder
						.add(Order.by(persistentProperty.getColumnName()).with(order.getNullHandling()).with(order.getDirection()));
			}
		}

		return Sort.by(mappedOrder);
	}

	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<T>((RelationalPersistentEntity) mappingContext.getRequiredPersistentEntity(typeToRead),
				instantiators, mappingContext);
	}

	@Override
	public String getTableName(Class<?> type) {
		return mappingContext.getRequiredPersistentEntity(type).getTableName();
	}
}
