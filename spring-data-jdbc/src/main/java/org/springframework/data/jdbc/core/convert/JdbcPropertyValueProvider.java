/*
 * Copyright 2020-2021 the original author or authors.
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
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.IdentifierProcessing;

/**
 * {@link PropertyValueProvider} obtaining values from a {@link ResultSetAccessor}.
 *
 * @author Jens Schauder
 * @since 2.0
 */
class JdbcPropertyValueProvider implements PropertyValueProvider<RelationalPersistentProperty> {

	private final IdentifierProcessing identifierProcessing;
	private final PersistentPropertyPathExtension basePath;
	private final ResultSetAccessor resultSet;

	/**
	 * @param identifierProcessing used for converting the
	 *          {@link org.springframework.data.relational.core.sql.SqlIdentifier} from a property to a column label
	 * @param basePath path from the aggregate root relative to which all properties get resolved.
	 * @param resultSet the {@link ResultSetAccessor} from which to obtain the actual values.
	 */
	JdbcPropertyValueProvider(IdentifierProcessing identifierProcessing, PersistentPropertyPathExtension basePath,
			ResultSetAccessor resultSet) {

		this.resultSet = resultSet;
		this.basePath = basePath;
		this.identifierProcessing = identifierProcessing;
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
		return basePath.extendBy(property).getColumnAlias().getReference();
	}

	public JdbcPropertyValueProvider extendBy(RelationalPersistentProperty property) {
		return new JdbcPropertyValueProvider(identifierProcessing, basePath.extendBy(property), resultSet);
	}
}
