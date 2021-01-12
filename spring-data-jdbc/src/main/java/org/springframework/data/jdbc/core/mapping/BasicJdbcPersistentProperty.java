/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.mapping;

import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.BasicRelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * @author Mark Paluch
 */
public class BasicJdbcPersistentProperty extends BasicRelationalPersistentProperty {

	/**
	 * Creates a new {@link BasicJdbcPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param context must not be {@literal null}
	 */
	public BasicJdbcPersistentProperty(Property property, PersistentEntity<?, RelationalPersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder, RelationalMappingContext context) {
		super(property, owner, simpleTypeHolder, context);
	}

	@Override
	public int getSqlType() {
		return JdbcUtil.sqlTypeFor(getColumnType());
	}

	@Override
	public boolean isReference() {
		return AggregateReference.class.isAssignableFrom(getRawType());
	}

}
