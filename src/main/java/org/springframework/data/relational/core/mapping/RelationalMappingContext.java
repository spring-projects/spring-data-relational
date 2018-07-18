/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link MappingContext} implementation for JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.0
 */
public class RelationalMappingContext extends AbstractMappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> {

	@Getter private final NamingStrategy namingStrategy;

	/**
	 * Creates a new {@link RelationalMappingContext}.
	 */
	public RelationalMappingContext() {
		this(NamingStrategy.INSTANCE);
	}

	/**
	 * Creates a new {@link RelationalMappingContext} using the given {@link NamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public RelationalMappingContext(NamingStrategy namingStrategy) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null!");

		this.namingStrategy = namingStrategy;

		setSimpleTypeHolder(new SimpleTypeHolder(Collections.emptySet(), true));
	}

	/**
	 * returns all {@link PropertyPath}s reachable from the root type in the order needed for deleting, i.e. the deepest
	 * reference first.
	 */
	public List<PropertyPath> referencedEntities(Class<?> rootType, @Nullable PropertyPath path) {

		List<PropertyPath> paths = new ArrayList<>();

		Class<?> currentType = path == null ? rootType : path.getLeafType();
		RelationalPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(currentType);

		for (RelationalPersistentProperty property : persistentEntity) {
			if (property.isEntity()) {

				PropertyPath nextPath = path == null ? PropertyPath.from(property.getName(), rootType)
						: path.nested(property.getName());
				paths.add(nextPath);
				paths.addAll(referencedEntities(rootType, nextPath));
			}
		}

		Collections.reverse(paths);

		return paths;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> RelationalPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new RelationalPersistentEntityImpl<>(typeInformation, this.namingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected RelationalPersistentProperty createPersistentProperty(Property property, RelationalPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {
		return new BasicRelationalPersistentProperty(property, owner, simpleTypeHolder, this);
	}
}
