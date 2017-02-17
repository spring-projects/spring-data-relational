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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * @author Jens Schauder
 */
public class JdbcPersistentProperty extends AnnotationBasedPersistentProperty<JdbcPersistentProperty> {

	/**
	 * Creates a new {@link AnnotationBasedPersistentProperty}.
	 *
	 * @param field must not be {@literal null}.
	 * @param propertyDescriptor can be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder
	 */
	public JdbcPersistentProperty(Field field, PropertyDescriptor propertyDescriptor, PersistentEntity<?, JdbcPersistentProperty> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}

	@Override
	protected Association<JdbcPersistentProperty> createAssociation() {
		return null;
	}
}
