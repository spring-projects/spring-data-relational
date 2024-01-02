/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * {@link PropertyValueProvider} obtaining values from a {@link ResultSetAccessor}. For a given id property it provides
 * the value in the resultset under which other entities refer back to it.
 *
 * @author Jens Schauder
 * @author Kurt Niemi
 * @author Mikhail Polivakha
 * @since 2.0
 */
class JdbcBackReferencePropertyValueProvider implements PropertyValueProvider<RelationalPersistentProperty> {

	private final AggregatePath basePath;
	private final ResultSetAccessor resultSet;

	/**
	 * @param basePath path from the aggregate root relative to which all properties get resolved.
	 * @param resultSet the {@link ResultSetAccessor} from which to obtain the actual values.
	 */
	JdbcBackReferencePropertyValueProvider(AggregatePath basePath, ResultSetAccessor resultSet) {

		this.resultSet = resultSet;
		this.basePath = basePath;
	}

	@Override
	public <T> T getPropertyValue(RelationalPersistentProperty property) {
		return (T) resultSet.getObject(basePath.append(property).getTableInfo().reverseColumnInfo().alias().getReference());
	}

	public JdbcBackReferencePropertyValueProvider extendBy(RelationalPersistentProperty property) {
		return new JdbcBackReferencePropertyValueProvider(basePath.append(property), resultSet);
	}
}
