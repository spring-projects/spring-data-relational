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

import org.springframework.data.mapping.InstanceCreatorMetadata;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link MappingContext} implementation for JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class JdbcMappingContext extends RelationalMappingContext {

	private static final String MISSING_PARAMETER_NAME = "A constructor parameter name must not be null to be used with Spring Data JDBC! Offending parameter: %s";

	/**
	 * Creates a new {@link JdbcMappingContext}.
	 */
	public JdbcMappingContext() {
		super();
		setSimpleTypeHolder(JdbcSimpleTypes.HOLDER);
	}

	/**
	 * Creates a new {@link JdbcMappingContext} using the given {@link NamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public JdbcMappingContext(NamingStrategy namingStrategy) {
		super(namingStrategy);
		setSimpleTypeHolder(JdbcSimpleTypes.HOLDER);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.RelationalMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> RelationalPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		RelationalPersistentEntity<T> entity = super.createPersistentEntity(typeInformation);
		InstanceCreatorMetadata<RelationalPersistentProperty> creator = entity.getInstanceCreatorMetadata();

		if (creator == null) {
			return entity;
		}

		for (Parameter<Object, RelationalPersistentProperty> parameter : creator.getParameters()) {
			Assert.state(StringUtils.hasText(parameter.getName()), () -> String.format(MISSING_PARAMETER_NAME, parameter));
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected RelationalPersistentProperty createPersistentProperty(Property property,
			RelationalPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		BasicJdbcPersistentProperty persistentProperty = new BasicJdbcPersistentProperty(property, owner, simpleTypeHolder,
				this.getNamingStrategy());
		persistentProperty.setForceQuote(isForceQuote());
		return persistentProperty;
	}

	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {

		return super.shouldCreatePersistentEntityFor(type) //
				&& !AggregateReference.class.isAssignableFrom(type.getType()) //
				&& !type.isCollectionLike();
	}

}
