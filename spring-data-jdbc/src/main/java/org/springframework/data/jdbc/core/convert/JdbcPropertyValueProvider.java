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
 * {@link PropertyValueProvider} obtaining values from a {@link ResultSetAccessor}.
 *
 * @author Jens Schauder
 * @author Kurt Niemi
 * @since 2.0
 */
class JdbcPropertyValueProvider implements PropertyValueProvider<RelationalPersistentProperty> {

	private final AggregatePath basePath;
	private final ResultSetAccessor resultSet;

	/**
	 * @param basePath path from the aggregate root relative to which all properties get resolved.
	 * @param resultSet the {@link ResultSetAccessor} from which to obtain the actual values.
	 */
	JdbcPropertyValueProvider(AggregatePath basePath, ResultSetAccessor resultSet) {

		this.resultSet = resultSet;
		this.basePath = basePath;
	}

	@Override
	public <T> T getPropertyValue(RelationalPersistentProperty property) {
		return (T) resultSet.getObject(getColumnName(property));
	}

	/**
	 * Returns whether the underlying source contains a data source for the given {@link RelationalPersistentProperty}.
	 *
	 * @param property
	 * @return
	 */
	public boolean hasProperty(RelationalPersistentProperty property) {
		return resultSet.hasValue(getColumnName(property));
	}

	private String getColumnName(RelationalPersistentProperty property) {
		AggregatePath.ColumnInfo columnInfo = basePath.append(property).getColumnInfo();
		return columnInfo.alias().getReference();
	}

	public JdbcPropertyValueProvider extendBy(RelationalPersistentProperty property) {
		return new JdbcPropertyValueProvider(basePath.append(property), resultSet);
	}
}
