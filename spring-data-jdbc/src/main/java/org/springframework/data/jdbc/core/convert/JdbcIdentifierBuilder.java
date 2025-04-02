/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.function.Function;

import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * Builder for {@link Identifier}. Mainly for internal use within the framework
 *
 * @author Jens Schauder
 * @since 1.1
 */
public class JdbcIdentifierBuilder {

	private Identifier identifier;

	private JdbcIdentifierBuilder(Identifier identifier) {
		this.identifier = identifier;
	}

	public static JdbcIdentifierBuilder empty() {
		return new JdbcIdentifierBuilder(Identifier.empty());
	}

	/**
	 * Creates ParentKeys with backreference for the given path and value of the parents id.
	 */
	public static JdbcIdentifierBuilder forBackReferences(JdbcConverter converter, AggregatePath path, Object value) {

		RelationalPersistentProperty idProperty = path.getIdDefiningParentPath().getRequiredIdProperty();
		AggregatePath.ColumnInfos infos = path.getTableInfo().reverseColumnInfos();

		// create property accessor
		RelationalMappingContext mappingContext = converter.getMappingContext();
		RelationalPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(idProperty.getType());

		Function<AggregatePath, Object> valueProvider;
		if (persistentEntity == null) {
			valueProvider = ap -> value;
		} else {
			PersistentPropertyPathAccessor<Object> propertyPathAccessor = persistentEntity.getPropertyPathAccessor(value);
			valueProvider = ap -> propertyPathAccessor.getProperty(ap.getRequiredPersistentPropertyPath());
		}

		Identifier identifierHolder = infos.reduce(Identifier.empty(), (ap, ci) -> {

			RelationalPersistentProperty property = ap.getRequiredLeafProperty();
			return Identifier.of(ci.name(), valueProvider.apply(ap),
					converter.getColumnType(property));
		}, Identifier::withPart);

		return new JdbcIdentifierBuilder(identifierHolder);
	}

	/**
	 * Adds a qualifier to the identifier to build. A qualifier is a map key or a list index.
	 *
	 * @param path path to the map that gets qualified by {@code value}. Must not be {@literal null}.
	 * @param value map key or list index qualifying the map identified by {@code path}. Must not be {@literal null}.
	 * @return this builder. Guaranteed to be not {@literal null}.
	 */
	public JdbcIdentifierBuilder withQualifier(AggregatePath path, Object value) {

		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		AggregatePath.TableInfo tableInfo = path.getTableInfo();
		identifier = identifier.withPart(tableInfo.qualifierColumnInfo().name(), value,
				tableInfo.qualifierColumnType());

		return this;
	}

	public Identifier build() {
		return identifier;
	}
}
