/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.lang.Nullable;

/**
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

	public static JdbcIdentifierBuilder from(Map<String, Object> additionalParameters) {

		Identifier[] identifier = new Identifier[] { Identifier.empty() };

		additionalParameters
				.forEach((k, v) -> identifier[0] = identifier[0].add(k, v, v == null ? Object.class : v.getClass()));

		return new JdbcIdentifierBuilder(identifier[0]);
	}

	/**
	 * Creates ParentKeys with backreference for the given path and value of the parents id.
	 */
	public static JdbcIdentifierBuilder forBackReferences(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable Object value) {

		Identifier identifier = Identifier.simple( //
				path.getRequiredLeafProperty().getReverseColumnName(), //
				value, //
				getLastIdProperty(path).getColumnType() //
		);

		return new JdbcIdentifierBuilder(identifier);
	}

	public JdbcIdentifierBuilder withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value) {

		RelationalPersistentProperty leafProperty = path.getRequiredLeafProperty();
		identifier = identifier.add(leafProperty.getKeyColumn(), value, leafProperty.getQualifierColumnType());

		return this;
	}

	public Identifier build() {
		return identifier;
	}

	private static RelationalPersistentProperty getLastIdProperty(
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		RelationalPersistentProperty idProperty = path.getRequiredLeafProperty().getOwner().getIdProperty();

		if (idProperty != null) {
			return idProperty;
		}

		return getLastIdProperty(path.getParentPath());
	}
}
