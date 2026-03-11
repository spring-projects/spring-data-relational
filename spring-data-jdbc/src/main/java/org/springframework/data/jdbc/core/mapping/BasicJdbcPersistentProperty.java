/*
 * Copyright 2018-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.BasicRelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Extension to {@link BasicRelationalPersistentProperty}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class BasicJdbcPersistentProperty extends BasicRelationalPersistentProperty {

	/**
	 * Creates a new {@link BasicJdbcPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param namingStrategy must not be {@literal null}
	 * @since 2.0
	 */
	public BasicJdbcPersistentProperty(Property property, PersistentEntity<?, RelationalPersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder, NamingStrategy namingStrategy) {
		super(property, owner, simpleTypeHolder, namingStrategy);
	}

	@Override
	public boolean isAssociation() {
		return super.isAssociation() || AggregateReference.class.isAssignableFrom(getRawType());
	}

	@Override
	public @Nullable TypeInformation<?> getAssociationTargetTypeInformation() {

		if (AggregateReference.class.isAssignableFrom(getRawType())) {
			return getTypeInformation().getComponentType();
		}

		return super.getAssociationTargetTypeInformation();
	}

	@Override
	protected TypeInformation<?> getActualTypeInformation() {

		if (AggregateReference.class.isAssignableFrom(getRawType())) {
			return getTypeInformation().getRequiredMapValueType();
		}

		return super.getActualTypeInformation();
	}
}
