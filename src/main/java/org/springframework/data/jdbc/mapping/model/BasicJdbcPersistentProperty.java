/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.jdbc.mapping.model;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Meta data about a property to be used by repository implementations.
 *
 * @author Jens Schauder
 * @since 2.0
 */
public class BasicJdbcPersistentProperty extends AnnotationBasedPersistentProperty<JdbcPersistentProperty>
		implements JdbcPersistentProperty {

	/**
	 * Creates a new {@link AnnotationBasedPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder
	 */
	public BasicJdbcPersistentProperty( //
			Property property, //
			PersistentEntity<?, JdbcPersistentProperty> owner, //
			SimpleTypeHolder simpleTypeHolder //
	) {
		super(property, owner, simpleTypeHolder);
	}

	@Override
	protected Association<JdbcPersistentProperty> createAssociation() {
		return null;
	}

	public String getColumnName() {
		return getName();
	}
}
