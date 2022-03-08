/*
 * Copyright 2017-2022 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * {@link MappingContext} implementation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class RelationalMappingContext
		extends AbstractMappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> {

	private final NamingStrategy namingStrategy;
	private boolean forceQuote = true;

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

		this.namingStrategy = new CachingNamingStrategy(namingStrategy);

		setSimpleTypeHolder(SimpleTypeHolder.DEFAULT);
	}

	/**
	 * Return whether quoting should be enabled for all table and column names. Quoting is enabled by default.
	 *
	 * @return
	 * @since 2.0
	 */
	public boolean isForceQuote() {
		return forceQuote;
	}

	/**
	 * Enable/disable quoting for all tables and column names.
	 *
	 * @param forceQuote
	 */
	public void setForceQuote(boolean forceQuote) {
		this.forceQuote = forceQuote;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> RelationalPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		RelationalPersistentEntityImpl<T> entity = new RelationalPersistentEntityImpl<>(typeInformation,
				this.namingStrategy);
		entity.setForceQuote(isForceQuote());

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected RelationalPersistentProperty createPersistentProperty(Property property,
			RelationalPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		BasicRelationalPersistentProperty persistentProperty = new BasicRelationalPersistentProperty(property, owner,
				simpleTypeHolder, this.namingStrategy);
		persistentProperty.setForceQuote(isForceQuote());

		return persistentProperty;
	}

	public NamingStrategy getNamingStrategy() {
		return this.namingStrategy;
	}
}
